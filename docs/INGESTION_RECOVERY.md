# Ingestion recovery

Producer reconnect exhaustion and provider authentication rejection now raise to
Prefect instead of returning success. Every failed WebSocket session is closed.
A session resets the consecutive failure budget only after 60 seconds and
acknowledged trades from all product symbols. Ping traffic alone is insufficient.

Each Kafka send waits for broker acknowledgement off the asyncio event loop.
`messages_sent` counts acknowledgements, not enqueue attempts. Delivery failure
raises into reconnect handling. Timeout outcomes can be ambiguous; downstream
deduplication is still required and this is not an exactly-once guarantee.

During active ingestion, any symbol without an acknowledged trade for 120 seconds
causes reconnect. The subscription warmup allowance starts at connection time.
Tune `finnhub_connection.trade_stale_seconds` and `recovery_seconds` in the YAML
for feed conditions; trading halts may also trigger freshness failure.

Producer health polling now survives startup and is cancelled/awaited on exit.
Metrics pushes include a registry and a five-second timeout. Set
`PUSHGATEWAY_URL=http://prometheus-pushgateway:9091` for container execution;
host execution defaults to `http://localhost:9091`.

Deploy on the next ingestion launch (rebuild the ingestion image if containerized).
No data replay, checkpoint reset, or historical-gap repair is performed by these
changes. Validate with a live session before claiming all-day feed reliability.
