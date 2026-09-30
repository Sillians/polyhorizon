import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from prefect import flow, get_run_logger

from polyhorizon.ingestion.configs.settings import load_config, Config
from polyhorizon.ingestion.utils.logger import configure_logging
from polyhorizon.ingestion.tasks.producer_consumer_streaming_tasks import (
    run_producer_task,
    run_consumer_task,
)
from polyhorizon.streaming.src.market_schedule import get_market_session
from polyhorizon.core.session_window import collection_session, scheduled_start


MARKET_TZ = ZoneInfo("America/New_York")
def _seconds_until_close(now: datetime) -> int:
    session = get_market_session(now, "NYSE", str(MARKET_TZ))
    if session is None:
        return 0
    _, close_dt = session
    if now >= close_dt:
        return 0
    return int((close_dt - now).total_seconds())


@flow(
    name="Finnhub-Ingestion-Pipeline",
    retries=2,
    retry_delay_seconds=60,
    timeout_seconds=7 * 60 * 60,
)
async def finnhub_ingestion_flow(config_path: str | None = None):
    logger = get_run_logger()
    cfg: Config = load_config(config_path)
    configure_logging(cfg.logging)

    now = datetime.now(MARKET_TZ)
    session = collection_session(now, scheduled_start())
    if session is None:
        logger.info("Outside the scheduled collection window; skipping.")
        return
    opening, close = session
    if now < opening:
        logger.info("Waiting for NYSE open at %s", opening)
        await asyncio.sleep((opening - now).total_seconds())
    now = datetime.now(MARKET_TZ)
    run_seconds = _seconds_until_close(now)
    if run_seconds <= 0:
        logger.info("Market is closed; skipping ingestion run.")
        return

    logger.info("Starting Finnhub ingestion flow for %.1f minutes.", run_seconds / 60)

    await run_pipeline(cfg, run_seconds, close.isoformat())


async def run_pipeline(cfg, run_seconds, stop_at=None):
    kwargs = {"stop_at": stop_at} if stop_at is not None else {}
    tasks = [asyncio.create_task(run_producer_task(cfg, run_seconds, **kwargs)),
             asyncio.create_task(run_consumer_task(cfg, run_seconds, **kwargs))]
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(finnhub_ingestion_flow())
