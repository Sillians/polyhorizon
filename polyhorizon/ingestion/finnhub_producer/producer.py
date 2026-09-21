from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import boto3
import websockets
from kafka import KafkaProducer
from kafka.errors import KafkaError

from polyhorizon.ingestion.configs.settings import Config, load_config
from polyhorizon.ingestion.utils.logger import get_logger, log_execution_time, log_function_call
from polyhorizon.sp500_data.reader import load_governed_universe


@dataclass(frozen=True)
class ProducerRuntimeConfig:
    finnhub_token: str
    finnhub_ws_url: str
    max_symbols_per_connection: int
    max_total_symbols: int
    subscription_delay: float
    connection_retry_delay: int
    rate_limit_delay: int
    max_retries: int
    ping_interval: int
    ping_timeout: int
    close_timeout: int
    max_message_size: int
    kafka_servers: List[str]
    kafka_topic: str
    producer_acks: str
    producer_retries: int
    producer_max_in_flight: int
    tickers_s3_path: str
    universe_current_key: str
    universe_max_age_hours: int
    seaweed_endpoint: str
    seaweed_access_key: str
    seaweed_secret_key: str
    metrics_interval: int

    @classmethod
    def from_settings(cls, settings: Config) -> "ProducerRuntimeConfig":
        finnhub = settings.finnhub_connection
        kafka = settings.kafka_connection
        bucket = settings.bucket_details
        monitoring = settings.monitoring_and_metrics

        if not kafka.kafka_servers:
            raise ValueError("kafka_servers must not be empty")

        return cls(
            finnhub_token=finnhub.finnhub_token.get_secret_value(),
            finnhub_ws_url=str(finnhub.finnhub_ws_url),
            max_symbols_per_connection=finnhub.max_symbols_per_connection,
            max_total_symbols=finnhub.max_total_symbols,
            subscription_delay=finnhub.subscription_delay,
            connection_retry_delay=finnhub.connection_retry_delay,
            rate_limit_delay=finnhub.rate_limit_delay,
            max_retries=finnhub.max_retries,
            ping_interval=finnhub.ping_interval,
            ping_timeout=finnhub.ping_timeout,
            close_timeout=finnhub.close_timeout,
            max_message_size=finnhub.max_message_size,
            kafka_servers=kafka.kafka_servers,
            kafka_topic=kafka.kafka_topic,
            producer_acks=kafka.producer_acks,
            producer_retries=kafka.producer_retries,
            producer_max_in_flight=kafka.producer_max_in_flight,
            tickers_s3_path=bucket.tickers_s3_path,
            universe_current_key=bucket.universe_current_key,
            universe_max_age_hours=bucket.universe_max_age_hours,
            seaweed_endpoint=str(bucket.seaweedfs_s3_endpoint),
            seaweed_access_key=bucket.seaweedfs_access_key.get_secret_value(),
            seaweed_secret_key=bucket.seaweedfs_secret_key.get_secret_value(),
            metrics_interval=monitoring.metrics_interval,
        )


class FinnhubProducer:
    """
    WebSocket producer for Finnhub market data -> Kafka.
    """

    def __init__(
        self,
        settings: Optional[Config] = None,
        runtime_config: Optional[ProducerRuntimeConfig] = None,
    ):
        self.settings = settings or load_config()
        self.config = runtime_config or ProducerRuntimeConfig.from_settings(self.settings)
        self.logger = get_logger("FinnhubProducer")

        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.config.seaweed_endpoint,
            aws_access_key_id=self.config.seaweed_access_key,
            aws_secret_access_key=self.config.seaweed_secret_key,
        )

        self._is_running = False
        self._websocket = None
        self._producer: Optional[KafkaProducer] = None
        self._subscribed_symbols: Set[str] = set()

        self._connection_start_time = 0.0
        self._messages_received = 0
        self._messages_sent = 0
        self._last_message_time = 0.0

        self._symbols: List[str] = []
        self._initialize()

    @log_function_call
    def _initialize(self) -> None:
        self._load_symbols_from_seaweed()
        self._create_kafka_producer()

    @staticmethod
    def _parse_s3_uri(uri: str) -> Tuple[str, str]:
        parsed = urlparse(uri)
        return parsed.netloc, parsed.path.lstrip("/")

    @log_function_call
    def _load_symbols_from_seaweed(self) -> None:
        bucket, _legacy_key = self._parse_s3_uri(self.config.tickers_s3_path)
        self.logger.info(
            "Resolving governed universe from s3://%s/%s",
            bucket,
            self.config.universe_current_key,
        )
        snapshot = load_governed_universe(
            self.s3_client,
            bucket,
            current_key=self.config.universe_current_key,
            maximum_age_hours=self.config.universe_max_age_hours,
        )
        from polyhorizon.core.product_symbols import ingestion_symbols
        self._symbols = ingestion_symbols(
            snapshot.symbols, self.config.max_total_symbols, self.config.max_symbols_per_connection
        )
        self.logger.info(
            "Loaded %d symbols from governed universe %s",
            len(self._symbols),
            snapshot.run_id,
        )

    @log_function_call
    def _create_kafka_producer(self) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=self.config.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=self.config.producer_acks,
            retries=self.config.producer_retries,
            max_in_flight_requests_per_connection=self.config.producer_max_in_flight,
            enable_idempotence=True,
            request_timeout_ms=30000,
            retry_backoff_ms=100,
        )
        self.logger.info("Kafka producer ready for topic %s", self.config.kafka_topic)

    async def _connect_websocket(self) -> None:
        self.logger.info("Connecting to Finnhub WebSocket")
        self._websocket = await websockets.connect(
            self.config.finnhub_ws_url,
            ping_interval=self.config.ping_interval,
            ping_timeout=self.config.ping_timeout,
            close_timeout=self.config.close_timeout,
            max_size=self.config.max_message_size,
        )
        self._connection_start_time = time.time()
        self._messages_received = 0
        self.logger.info("WebSocket connected")

    async def _subscribe_to_symbols(self, symbols: List[str]) -> None:
        if not self._websocket:
            raise RuntimeError("WebSocket not connected")

        limited_symbols = symbols[: self.config.max_symbols_per_connection]
        if len(symbols) > self.config.max_symbols_per_connection:
            self.logger.warning(
                "Limiting subscription from %d to %d symbols",
                len(symbols),
                self.config.max_symbols_per_connection,
            )

        for i, symbol in enumerate(limited_symbols):
            if i > 0:
                await asyncio.sleep(self.config.subscription_delay)
            await self._websocket.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            self._subscribed_symbols.add(symbol)
        self.logger.info("Subscribed to %d symbols", len(limited_symbols))

    async def _handle_websocket_message(self, raw_message: str) -> None:
        self._messages_received += 1
        self._last_message_time = time.time()

        try:
            message = json.loads(raw_message)
        except json.JSONDecodeError:
            self.logger.debug("Failed to parse JSON message")
            return

        message_type = message.get("type", "unknown")
        if message_type == "ping":
            await self._handle_ping_message()
        elif message_type == "trade":
            await self._handle_trade_message(message)

    async def _handle_ping_message(self) -> None:
        if self._websocket:
            await self._websocket.send(json.dumps({"type": "pong"}))

    async def _handle_trade_message(self, message: Dict[str, Any]) -> None:
        data = message.get("data")
        if not isinstance(data, list):
            return
        for trade_data in data:
            await self._process_trade_data(trade_data)

    async def _process_trade_data(self, trade_data: Dict[str, Any]) -> None:
        symbol = trade_data.get("s") or trade_data.get("symbol")
        price = trade_data.get("p") or trade_data.get("price")
        volume = trade_data.get("v") or trade_data.get("size")
        timestamp = trade_data.get("t") or trade_data.get("timestamp")

        if not symbol or price is None:
            return

        enriched_trade = {
            "symbol": symbol,
            "price": float(price),
            "volume": float(volume) if volume is not None else 0.0,
            "timestamp": int(timestamp) if timestamp else int(time.time() * 1000),
            "source": "finnhub",
            "producer_id": os.getpid(),
            "received_at": int(time.time() * 1000),
            "connection_id": id(self._websocket),
        }

        await self._send_to_kafka(symbol, enriched_trade)

    async def _send_to_kafka(self, key: str, message: Dict[str, Any]) -> None:
        try:
            self._producer.send(self.config.kafka_topic, key=key, value=message)
            self._messages_sent += 1
        except KafkaError as e:
            self.logger.error("Kafka send error: %s", e)
        except Exception as e:
            self.logger.error("Unexpected error sending to Kafka: %s", e)

    def _log_health_metrics(self) -> None:
        if self._connection_start_time <= 0:
            return
        uptime = time.time() - self._connection_start_time
        rate = self._messages_received / uptime if uptime > 0 else 0
        self.logger.info(
            "Health: %d received, %d sent, %.1fs uptime, %.2f msg/sec",
            self._messages_received,
            self._messages_sent,
            uptime,
            rate,
        )

    async def _run_websocket_loop(self) -> None:
        try:
            async for raw_message in self._websocket:
                if not self._is_running:
                    break
                await self._handle_websocket_message(raw_message)
                if self._messages_received % self.config.metrics_interval == 0:
                    self._log_health_metrics()
        except websockets.exceptions.ConnectionClosed as e:
            duration = time.time() - self._connection_start_time
            self.logger.warning(
                "WebSocket closed after %.1fs, %d messages: %s",
                duration,
                self._messages_received,
                e,
            )
            raise

    async def _handle_connection_with_retries(self) -> None:
        retry_count = 0
        while retry_count < self.config.max_retries and self._is_running:
            try:
                await self._connect_websocket()
                await self._subscribe_to_symbols(self._symbols)
                await self._run_websocket_loop()
                retry_count = 0
            except websockets.exceptions.InvalidStatus as e:
                if "401" in str(e):
                    self.logger.error("Authentication failed (HTTP 401). Check FINNHUB_TOKEN.")
                    break
                if "429" in str(e):
                    retry_count += 1
                    await asyncio.sleep(self.config.rate_limit_delay)
                else:
                    retry_count += 1
                    await asyncio.sleep(self.config.connection_retry_delay * retry_count)
            except (websockets.exceptions.ConnectionClosedError, websockets.exceptions.WebSocketException):
                retry_count += 1
                await asyncio.sleep(self.config.connection_retry_delay * retry_count)
            except Exception:
                retry_count += 1
                await asyncio.sleep(self.config.connection_retry_delay * retry_count)

        if retry_count >= self.config.max_retries:
            self.logger.error("Max retries exceeded. Stopping.")

    @log_execution_time
    async def start(self) -> None:
        if self._is_running:
            self.logger.warning("Producer is already running")
            return

        self.logger.info(
            "Starting producer | symbols=%d delay=%.2fs topic=%s",
            self.config.max_symbols_per_connection,
            self.config.subscription_delay,
            self.config.kafka_topic,
        )

        self._is_running = True
        try:
            await self._handle_connection_with_retries()
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        finally:
            await self.stop()

    async def stop(self) -> None:
        if not self._is_running:
            return
        self._is_running = False

        try:
            if self._websocket:
                await self._websocket.close()
            if self._producer:
                self._producer.flush(timeout=5)
                self._producer.close()
            if self._connection_start_time > 0:
                self._log_health_metrics()
            self.logger.info("FinnhubProducer stopped successfully")
        except Exception as e:
            self.logger.error("Error during shutdown: %s", e)

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def subscribed_symbols(self) -> Set[str]:
        return self._subscribed_symbols.copy()

    @property
    def metrics(self) -> Dict[str, Any]:
        uptime = time.time() - self._connection_start_time if self._connection_start_time > 0 else 0
        return {
            "messages_received": self._messages_received,
            "messages_sent": self._messages_sent,
            "uptime_seconds": uptime,
            "message_rate": self._messages_received / uptime if uptime > 0 else 0,
            "subscribed_symbols_count": len(self._subscribed_symbols),
            "is_running": self._is_running,
            "last_message_age": time.time() - self._last_message_time if self._last_message_time > 0 else None,
        }
