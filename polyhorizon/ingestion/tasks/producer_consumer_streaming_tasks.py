import asyncio
import time
import os
from datetime import datetime
from contextlib import suppress
from typing import Optional
from prefect import task, get_run_logger
from prometheus_client import Gauge, push_to_gateway, REGISTRY

from polyhorizon.ingestion.configs.settings import Config
from polyhorizon.ingestion.finnhub_producer.producer import FinnhubProducer
from polyhorizon.ingestion.finnhub_consumer.consumer import FinnhubConsumer

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "http://localhost:9091")
SYMBOL_AGE = Gauge("ingestion_producer_symbol_trade_age_seconds", "Seconds since acknowledged trade", ["symbol"])
PRODUCER_METRICS = {
    "messages_received": Gauge("ingestion_producer_messages_received", "Producer messages received"),
    "messages_sent": Gauge("ingestion_producer_messages_sent", "Producer messages sent"),
    "message_rate": Gauge("ingestion_producer_message_rate", "Producer message rate"),
}
CONSUMER_METRICS = {
    "messages_consumed": Gauge("ingestion_consumer_messages_consumed", "Consumer messages consumed"),
    "messages_processed": Gauge("ingestion_consumer_messages_processed", "Consumer messages processed"),
    "messages_failed": Gauge("ingestion_consumer_messages_failed", "Consumer messages failed"),
    "consume_rate": Gauge("ingestion_consumer_consume_rate", "Consumer consume rate"),
    "process_rate": Gauge("ingestion_consumer_process_rate", "Consumer process rate"),
    "error_rate": Gauge("ingestion_consumer_error_rate", "Consumer error rate"),
}

@task(retries=3, retry_delay_seconds=[10, 30, 60], name="Stream-Producer")
async def run_producer_task(raw_cfg: Config, run_seconds: Optional[int] = None, stop_at: str | None = None):
    if stop_at is not None:
        run_seconds = datetime.fromisoformat(stop_at).timestamp() - time.time()
        if run_seconds <= 0:
            return
    logger = get_run_logger()
    producer = FinnhubProducer(settings=raw_cfg)
    health_interval = raw_cfg.monitoring_and_metrics.health_check_interval

    async def _health_loop():
        while True:
            await asyncio.to_thread(producer_health_check_task.fn, producer.metrics)
            await asyncio.sleep(health_interval)
    
    logger.info("Starting Finnhub WebSocket producer...")
    try:
        health_task = asyncio.create_task(_health_loop())
        if run_seconds:
            await asyncio.wait_for(producer.start(), timeout=run_seconds)
        else:
            await producer.start()
    except asyncio.TimeoutError:
        logger.info("Market session elapsed; stopping producer.")
    except Exception as e:
        logger.error(f"Producer encountered an error: {e}")
        raise
    finally:
        if "health_task" in locals():
            health_task.cancel()
        try:
            if "health_task" in locals():
                with suppress(asyncio.CancelledError):
                    await health_task
        finally:
            await producer.stop()

@task(retries=3, retry_delay_seconds=[10, 30, 60], name="Stream-Consumer")
async def run_consumer_task(raw_cfg: Config, run_seconds: Optional[int] = None, stop_at: str | None = None):
    logger = get_run_logger()
    health_interval = raw_cfg.monitoring_and_metrics.health_check_interval
    end_time = datetime.fromisoformat(stop_at).timestamp() if stop_at else (time.time() + run_seconds if run_seconds else None)
    if end_time is not None and end_time <= time.time():
        return
    
    # We use the context manager here to ensure Kafka partition rebalancing 
    # happens correctly if the task is killed or retried.
    with FinnhubConsumer(settings=raw_cfg) as consumer:
        logger.info("Starting Kafka consumer loop...")
        try:
            consumer.start(blocking=False)
            while consumer.is_running:
                await asyncio.to_thread(consumer_health_check_task.fn, consumer.metrics)
                if end_time and time.time() >= end_time:
                    logger.info("Market session elapsed; stopping consumer.")
                    break
                await asyncio.sleep(min(health_interval, max(0, end_time - time.time())) if end_time else health_interval)
            if consumer.thread_error is not None:
                raise RuntimeError("Kafka consumer stopped after a processing failure") from consumer.thread_error
        except Exception as e:
            logger.error(f"Consumer encountered an error: {e}")
            raise


@task(name="Producer-Health-Check")
def producer_health_check_task(metrics: dict) -> None:
    logger = get_run_logger()
    logger.info("Producer health: %s", metrics)
    try:
        PRODUCER_METRICS["messages_received"].set(metrics.get("messages_received", 0))
        PRODUCER_METRICS["messages_sent"].set(metrics.get("messages_sent", 0))
        PRODUCER_METRICS["message_rate"].set(metrics.get("message_rate", 0))
        for symbol, age in metrics.get("symbol_trade_age_seconds", {}).items():
            SYMBOL_AGE.labels(symbol=symbol).set(age)
        push_to_gateway(PUSHGATEWAY_URL, job="ingestion_producer", registry=REGISTRY, timeout=5)
    except Exception as exc:
        logger.warning("Failed to push producer metrics: %s", exc)


@task(name="Consumer-Health-Check")
def consumer_health_check_task(metrics: dict) -> None:
    logger = get_run_logger()
    logger.info("Consumer health: %s", metrics)
    try:
        CONSUMER_METRICS["messages_consumed"].set(metrics.get("messages_consumed", 0))
        CONSUMER_METRICS["messages_processed"].set(metrics.get("messages_processed", 0))
        CONSUMER_METRICS["messages_failed"].set(metrics.get("messages_failed", 0))
        CONSUMER_METRICS["consume_rate"].set(metrics.get("consume_rate", 0))
        CONSUMER_METRICS["process_rate"].set(metrics.get("process_rate", 0))
        CONSUMER_METRICS["error_rate"].set(metrics.get("error_rate", 0))
        push_to_gateway(PUSHGATEWAY_URL, job="ingestion_consumer", registry=REGISTRY, timeout=5)
    except Exception as exc:
        logger.warning("Failed to push consumer metrics: %s", exc)
