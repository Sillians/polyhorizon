from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from kafka import KafkaConsumer
from kafka.errors import KafkaError, KafkaTimeoutError

from polyhorizon.ingestion.configs.settings import Config, load_config
from polyhorizon.ingestion.utils.logger import get_logger, log_execution_time, log_function_call


@dataclass(frozen=True)
class ConsumerRuntimeConfig:
    kafka_servers: List[str]
    kafka_topic: str
    consumer_group_id: str
    auto_offset_reset: str
    enable_auto_commit: bool
    auto_commit_interval_ms: int
    session_timeout_ms: int
    heartbeat_interval_ms: int
    max_poll_records: int
    consumer_timeout_ms: int
    batch_processing: bool
    batch_size: int
    processing_timeout: float
    metrics_interval: int
    health_check_interval: float
    max_consecutive_errors: int
    error_backoff_seconds: float

    @classmethod
    def from_settings(cls, settings: Config) -> "ConsumerRuntimeConfig":
        kafka = settings.kafka_connection
        perf = settings.load_consumer_performance
        proc = settings.load_processing_configuration
        mon = settings.monitoring_and_metrics
        err = settings.error_handling

        if not kafka.kafka_servers:
            raise ValueError("kafka_servers must not be empty")

        return cls(
            kafka_servers=kafka.kafka_servers,
            kafka_topic=kafka.kafka_topic,
            consumer_group_id=kafka.consumer_group_id,
            auto_offset_reset=perf.auto_offset_reset,
            enable_auto_commit=perf.enable_auto_commit,
            auto_commit_interval_ms=perf.auto_commit_interval_ms,
            session_timeout_ms=perf.session_timeout_ms,
            heartbeat_interval_ms=perf.heartbeat_interval_ms,
            max_poll_records=perf.max_poll_records,
            consumer_timeout_ms=perf.consumer_timeout_ms,
            batch_processing=proc.batch_processing,
            batch_size=proc.batch_size,
            processing_timeout=proc.processing_timeout,
            metrics_interval=mon.metrics_interval,
            health_check_interval=mon.health_check_interval,
            max_consecutive_errors=err.max_consecutive_errors,
            error_backoff_seconds=err.error_backoff_seconds,
        )


@dataclass
class TradeMessage:
    raw_data: Dict[str, Any]
    key: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None
    consumed_at: int = field(default_factory=lambda: int(time.time() * 1000))
    symbol: Optional[str] = None
    price: Optional[float] = None
    volume: Optional[float] = None
    timestamp: Optional[int] = None
    source: Optional[str] = None
    producer_id: Optional[str] = None
    received_at: Optional[int] = None
    connection_id: Optional[str] = None
    end_to_end_latency_ms: Optional[int] = None
    market_latency_ms: Optional[int] = None

    def __post_init__(self) -> None:
        self._extract_trade_data()

    def _extract_trade_data(self) -> None:
        self.symbol = self.raw_data.get("symbol") or self.key
        self.price = self._safe_float(self.raw_data.get("price"))
        self.volume = self._safe_float(self.raw_data.get("volume", 0.0))
        self.timestamp = self._safe_int(self.raw_data.get("timestamp", 0))
        self.source = self.raw_data.get("source", "unknown")
        self.producer_id = self.raw_data.get("producer_id")
        self.received_at = self._safe_int(self.raw_data.get("received_at", 0))
        self.connection_id = self.raw_data.get("connection_id")

        if self.received_at:
            self.end_to_end_latency_ms = self.consumed_at - self.received_at
        if self.timestamp:
            self.market_latency_ms = self.consumed_at - self.timestamp

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    def is_valid(self) -> bool:
        return self.symbol is not None and self.price is not None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "price": self.price,
            "volume": self.volume,
            "timestamp": self.timestamp,
            "source": self.source,
            "producer_id": self.producer_id,
            "received_at": self.received_at,
            "consumed_at": self.consumed_at,
            "connection_id": self.connection_id,
            "end_to_end_latency_ms": self.end_to_end_latency_ms,
            "market_latency_ms": self.market_latency_ms,
            "partition": self.partition,
            "offset": self.offset,
            "key": self.key,
        }

    def __str__(self) -> str:
        latency_info = f"E2E: {self.end_to_end_latency_ms}ms" if self.end_to_end_latency_ms else "E2E: N/A"
        return f"{self.symbol}: ${self.price} (vol: {self.volume}) [{latency_info}] from {self.source}"


class FinnhubConsumer:
    """
    Kafka consumer for real-time stock trade data.
    """

    def __init__(
        self,
        settings: Optional[Config] = None,
        runtime_config: Optional[ConsumerRuntimeConfig] = None,
        consumer_factory: Optional[Callable[[ConsumerRuntimeConfig], KafkaConsumer]] = None,
    ):
        self.settings = settings or load_config()
        self.config = runtime_config or ConsumerRuntimeConfig.from_settings(self.settings)
        self._consumer_factory = consumer_factory or self._default_consumer_factory
        self.logger = get_logger("FinnhubConsumer")

        self._is_running = False
        self._consumer: Optional[KafkaConsumer] = None
        self._processing_thread: Optional[threading.Thread] = None
        self._shutdown_event = threading.Event()

        self._messages_consumed = 0
        self._messages_processed = 0
        self._messages_failed = 0
        self._start_time = 0.0
        self._last_message_time = 0.0
        self._consecutive_errors = 0
        self._symbols_seen: Set[str] = set()
        self._partition_offsets: Dict[int, int] = {}

        self._message_handlers: List[Callable[[TradeMessage], None]] = []
        self._batch_handlers: List[Callable[[List[TradeMessage]], None]] = []
        self._error_handlers: List[Callable[[Exception, Optional[TradeMessage]], None]] = []

        self._initialize()

    @log_function_call
    def _initialize(self) -> None:
        self._consumer = self._consumer_factory(self.config)
        self.logger.info(
            "Kafka consumer ready | topic=%s group=%s",
            self.config.kafka_topic,
            self.config.consumer_group_id,
        )

    def _default_consumer_factory(self, cfg: ConsumerRuntimeConfig) -> KafkaConsumer:
        return KafkaConsumer(
            cfg.kafka_topic,
            bootstrap_servers=cfg.kafka_servers,
            group_id=cfg.consumer_group_id,
            auto_offset_reset=cfg.auto_offset_reset,
            enable_auto_commit=cfg.enable_auto_commit,
            auto_commit_interval_ms=cfg.auto_commit_interval_ms,
            session_timeout_ms=cfg.session_timeout_ms,
            heartbeat_interval_ms=cfg.heartbeat_interval_ms,
            max_poll_records=cfg.max_poll_records,
            consumer_timeout_ms=cfg.consumer_timeout_ms,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
            key_deserializer=lambda x: x.decode("utf-8") if x else None,
            fetch_min_bytes=1,
            fetch_max_wait_ms=500,
            max_partition_fetch_bytes=1048576,
        )

    def add_message_handler(self, handler: Callable[[TradeMessage], None]) -> None:
        self._message_handlers.append(handler)
        self.logger.info("Added message handler: %s", handler.__name__)

    def add_batch_handler(self, handler: Callable[[List[TradeMessage]], None]) -> None:
        self._batch_handlers.append(handler)
        self.logger.info("Added batch handler: %s", handler.__name__)

    def add_error_handler(self, handler: Callable[[Exception, Optional[TradeMessage]], None]) -> None:
        self._error_handlers.append(handler)
        self.logger.info("Added error handler: %s", handler.__name__)

    def _default_message_handler(self, trade: TradeMessage) -> None:
        if not trade.is_valid():
            self.logger.warning("Invalid trade message: %s", trade)
            return
        latency_str = f"{trade.end_to_end_latency_ms}ms" if trade.end_to_end_latency_ms else "N/A"
        self.logger.info(
            "%s | $%s | Vol: %s | Latency: %s | Source: %s",
            trade.symbol,
            trade.price,
            trade.volume,
            latency_str,
            trade.source,
        )

    def _process_message(self, message) -> None:
        try:
            trade = TradeMessage(
                raw_data=message.value or {},
                key=message.key,
                partition=message.partition,
                offset=message.offset,
            )
            self._messages_consumed += 1
            self._last_message_time = time.time()
            if trade.symbol:
                self._symbols_seen.add(trade.symbol)
            if message.partition is not None:
                self._partition_offsets[message.partition] = message.offset

            if self._message_handlers:
                for handler in self._message_handlers:
                    try:
                        handler(trade)
                    except Exception as e:
                        self.logger.error("Error in message handler %s: %s", handler.__name__, e)
                        self._handle_processing_error(e, trade)
            else:
                self._default_message_handler(trade)

            self._messages_processed += 1
            self._consecutive_errors = 0
        except Exception as e:
            self._messages_failed += 1
            self._consecutive_errors += 1
            self.logger.error("Failed to process message: %s", e)
            self._handle_processing_error(e, None)

    def _process_message_batch(self, messages: List) -> None:
        trades: List[TradeMessage] = []
        for message in messages:
            try:
                trade = TradeMessage(
                    raw_data=message.value or {},
                    key=message.key,
                    partition=message.partition,
                    offset=message.offset,
                )
                trades.append(trade)
                self._messages_consumed += 1
                if trade.symbol:
                    self._symbols_seen.add(trade.symbol)
                if message.partition is not None:
                    self._partition_offsets[message.partition] = message.offset
            except Exception as e:
                self.logger.error("Failed to build trade from batch item: %s", e)
                self._messages_failed += 1

        self._last_message_time = time.time()

        if self._batch_handlers:
            for handler in self._batch_handlers:
                try:
                    handler(trades)
                except Exception as e:
                    self.logger.error("Error in batch handler %s: %s", handler.__name__, e)
                    self._handle_processing_error(e, None)
        else:
            for trade in trades:
                if self._message_handlers:
                    for handler in self._message_handlers:
                        try:
                            handler(trade)
                        except Exception as e:
                            self.logger.error("Error in message handler %s: %s", handler.__name__, e)
                            self._handle_processing_error(e, trade)
                else:
                    self._default_message_handler(trade)

        self._messages_processed += len(trades)
        self._consecutive_errors = 0

    def _handle_processing_error(self, error: Exception, trade: Optional[TradeMessage] = None) -> None:
        if self._error_handlers:
            for handler in self._error_handlers:
                try:
                    handler(error, trade)
                except Exception as e:
                    self.logger.error("Error in error handler %s: %s", handler.__name__, e)

        if self._consecutive_errors >= self.config.max_consecutive_errors:
            self.logger.error(
                "Too many consecutive errors (%d). Backing off for %.1fs.",
                self._consecutive_errors,
                self.config.error_backoff_seconds,
            )
            time.sleep(self.config.error_backoff_seconds)
            self._consecutive_errors = 0

    def _log_health_metrics(self) -> None:
        if self._start_time <= 0:
            return
        uptime = time.time() - self._start_time
        consume_rate = self._messages_consumed / uptime if uptime > 0 else 0
        process_rate = self._messages_processed / uptime if uptime > 0 else 0
        message_age = time.time() - self._last_message_time if self._last_message_time > 0 else None

        if message_age is None:
            message_age_info = "no recent msgs"
        else:
            message_age_info = f"{message_age:.1f}s ago"

        self.logger.info(
            "Consumer Health: %d consumed, %d processed, %d failed, %.2f c/sec, %.2f p/sec, %d symbols, last msg: %s",
            self._messages_consumed,
            self._messages_processed,
            self._messages_failed,
            consume_rate,
            process_rate,
            len(self._symbols_seen),
            message_age_info,
        )

        if self._partition_offsets:
            partition_info = ", ".join([f"P{p}:{o}" for p, o in self._partition_offsets.items()])
            self.logger.debug("Partition offsets: %s", partition_info)

    def _consumer_loop(self) -> None:
        try:
            if self.config.batch_processing:
                batch: List[Any] = []
                for message in self._consumer:
                    if self._shutdown_event.is_set():
                        break
                    batch.append(message)
                    if len(batch) >= self.config.batch_size:
                        self._process_message_batch(batch)
                        batch = []
                        if self._messages_consumed % self.config.metrics_interval == 0:
                            self._log_health_metrics()

                if batch:
                    self._process_message_batch(batch)
            else:
                for message in self._consumer:
                    if self._shutdown_event.is_set():
                        break
                    self._process_message(message)
                    if self._messages_consumed % self.config.metrics_interval == 0:
                        self._log_health_metrics()

        except KafkaTimeoutError:
            if not self._shutdown_event.is_set():
                self.logger.debug("Consumer timeout - no messages available")
        except KafkaError as e:
            self.logger.error("Kafka error in consumer loop: %s", e)
            raise
        except Exception as e:
            self.logger.error("Unexpected error in consumer loop: %s", e)
            raise

    @log_execution_time
    def start(self, blocking: bool = True) -> None:
        if self._is_running:
            self.logger.warning("Consumer is already running")
            return

        self.logger.info(
            "Starting consumer | topic=%s group=%s batch=%s handlers=%d+%d",
            self.config.kafka_topic,
            self.config.consumer_group_id,
            self.config.batch_processing,
            len(self._message_handlers),
            len(self._batch_handlers),
        )

        self._is_running = True
        self._start_time = time.time()
        self._shutdown_event.clear()

        try:
            if blocking:
                self._consumer_loop()
            else:
                self._processing_thread = threading.Thread(
                    target=self._consumer_loop,
                    name="FinnhubConsumerThread",
                    daemon=True,
                )
                self._processing_thread.start()
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        except Exception as e:
            self.logger.error("Consumer failed: %s", e)
            raise
        finally:
            if blocking:
                self.stop()

    def stop(self) -> None:
        if not self._is_running:
            return
        self._is_running = False
        self._shutdown_event.set()

        try:
            if self._processing_thread and self._processing_thread.is_alive():
                self._processing_thread.join(timeout=self.config.processing_timeout)
                if self._processing_thread.is_alive():
                    self.logger.warning("Consumer thread did not stop within timeout")

            if self._consumer:
                self._consumer.close()

            if self._start_time > 0:
                self._log_health_metrics()

            self.logger.info("FinnhubConsumer stopped successfully")
        except Exception as e:
            self.logger.error("Error during shutdown: %s", e)

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def metrics(self) -> Dict[str, Any]:
        uptime = time.time() - self._start_time if self._start_time > 0 else 0
        return {
            "messages_consumed": self._messages_consumed,
            "messages_processed": self._messages_processed,
            "messages_failed": self._messages_failed,
            "uptime_seconds": uptime,
            "consume_rate": self._messages_consumed / uptime if uptime > 0 else 0,
            "process_rate": self._messages_processed / uptime if uptime > 0 else 0,
            "error_rate": self._messages_failed / self._messages_consumed if self._messages_consumed > 0 else 0,
            "unique_symbols_seen": len(self._symbols_seen),
            "symbols_seen": list(self._symbols_seen),
            "partition_offsets": self._partition_offsets.copy(),
            "consecutive_errors": self._consecutive_errors,
            "is_running": self._is_running,
            "last_message_age": time.time() - self._last_message_time if self._last_message_time > 0 else None,
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
