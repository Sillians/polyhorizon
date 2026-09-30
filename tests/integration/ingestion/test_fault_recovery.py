"""Real local WebSocket + Kafka acknowledgements, no Finnhub calls or production topic."""
import asyncio
import json
import logging
import os
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest

pytestmark = pytest.mark.skipif(os.getenv("INGESTION_FAULT_E2E") != "1", reason="Requires local Kafka")


def test_disconnect_then_provider_error_fails_with_acknowledged_records():
    import websockets
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    from polyhorizon.ingestion.finnhub_producer.producer import FinnhubProducer
    servers = os.getenv("RECOVERY_KAFKA_SERVERS", "localhost:9092,localhost:9093,localhost:9094").split(",")
    topic = "reliability-test-" + uuid4().hex
    admin = KafkaAdminClient(bootstrap_servers=servers)
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    p = object.__new__(FinnhubProducer)
    p.logger = logging.getLogger("test-producer")
    p._is_running = False
    p._websocket = None
    p._producer = None
    p._subscribed_symbols = set()
    p._symbols = ["NVDA", "AAPL", "MSFT"]
    p._connection_start_time = p._last_message_time = 0
    p._messages_received = p._messages_sent = 0
    p._last_trades = {}
    p._session_healthy = False
    connections = []

    async def exercise():
        async def upstream(ws):
            connections.append(1)
            for _ in range(3):
                await ws.recv()
            if len(connections) == 1:
                await ws.send(json.dumps({"type": "trade", "data": [
                    {"s": s, "p": 100, "v": 10, "t": int(time.time() * 1000)} for s in p._symbols]}))
                # Allow real acknowledgements and the healthy-session check.
                while p._messages_sent < 3:
                    await asyncio.sleep(.02)
                await asyncio.sleep(.05)
                await ws.close(code=1011, reason="injected disconnect")
            else:
                await ws.send(json.dumps({"type": "error", "msg": "injected provider rejection"}))
                await ws.wait_closed()
        async with websockets.serve(upstream, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            p.config = SimpleNamespace(finnhub_ws_url=f"ws://127.0.0.1:{port}", ping_interval=10,
                ping_timeout=10, close_timeout=1, max_message_size=100000, max_symbols_per_connection=3,
                subscription_delay=0, connection_retry_delay=0, rate_limit_delay=0, max_retries=2,
                recovery_seconds=0, trade_stale_seconds=30, metrics_interval=100,
                kafka_servers=servers, kafka_topic=topic, producer_acks="all", producer_retries=3,
                producer_max_in_flight=1)
            p._create_kafka_producer()
            with pytest.raises(RuntimeError, match="budget exhausted"):
                await asyncio.wait_for(p.start(), timeout=30)
    try:
        asyncio.run(exercise())
        assert len(connections) == 2
        assert p._messages_sent == 3
        assert not p.is_running
        c = KafkaConsumer(topic, bootstrap_servers=servers, group_id=None, enable_auto_commit=False,
                          auto_offset_reset="earliest", consumer_timeout_ms=5000)
        try:
            rows = [json.loads(m.value) for m in c]
            assert {r["symbol"] for r in rows} == set(p._symbols)
            assert len(rows) == 3
        finally:
            c.close()
    finally:
        admin.delete_topics([topic])
        admin.close()
