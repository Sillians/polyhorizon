from __future__ import annotations

from typing import Optional

from prefect import flow, get_run_logger, task

from polyhorizon.sp500_data.configs.settings import load_config
from polyhorizon.sp500_data.get_sp500_companies import SP500Scraper


@task(
    name="Publish Governed SP500 Universe",
    retries=3,
    retry_delay_seconds=[30, 120, 300],
    timeout_seconds=15 * 60,
)
def publish_universe_task(config_path: Optional[str] = None) -> dict:
    return SP500Scraper(config=load_config(config_path)).run()


@flow(
    name="SP500 Universe Refresh",
    retries=1,
    retry_delay_seconds=600,
    timeout_seconds=30 * 60,
)
def sp500_universe_flow(config_path: Optional[str] = None) -> dict:
    """Refresh the canonical universe before downstream market-data ingestion."""
    logger = get_run_logger()
    manifest = publish_universe_task(config_path)
    logger.info(
        "Universe activated: run_id=%s count=%s additions=%s removals=%s",
        manifest["run_id"],
        manifest["constituent_count"],
        len(manifest["changes"]["additions"]),
        len(manifest["changes"]["removals"]),
    )
    return manifest


if __name__ == "__main__":
    sp500_universe_flow()
