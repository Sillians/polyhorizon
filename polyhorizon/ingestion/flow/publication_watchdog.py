"""Read-only calendar-aware publication deadline check, independent of serving traffic."""
import os
import re
from datetime import datetime, timedelta, timezone

import asyncpg
import pandas_market_calendars as mcal
from dotenv import load_dotenv
from prefect import flow
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


def expected_close(now, delay_minutes=60):
    schedule = mcal.get_calendar("NYSE").schedule(start_date=(now - timedelta(days=32)).date(),
                                                 end_date=now.date())
    due = schedule.market_close[schedule.market_close + timedelta(minutes=delay_minutes) <= now]
    if due.empty:
        raise ValueError("No completed NYSE session available in calendar")
    return due.iloc[-1].to_pydatetime()


@flow(name="publication-deadline-watchdog", retries=2, retry_delay_seconds=30)
async def publication_deadline_watchdog(delay_minutes: int = 60):
    load_dotenv()
    if not 1 <= delay_minutes <= 240:
        raise ValueError("Publication allowance must be 1..240 minutes")
    required = expected_close(datetime.now(timezone.utc), delay_minutes)
    schema = os.environ["FEAST_SCHEMA"]
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError("Invalid feature schema")
    conn = await asyncpg.connect(host=os.environ["POSTGRES_HOST"], port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.environ["POSTGRES_FEAST_DB"], user=os.environ["POSTGRES_FEAST_USER"],
        password=os.environ["POSTGRES_FEAST_PASSWORD"], timeout=10)
    try:
        exists = await conn.fetchval("SELECT to_regclass($1)", f"{schema}.dataset_publications")
        published = await conn.fetchval(
            f"SELECT MAX(max_event_time) FROM {schema}.dataset_publications WHERE status='complete'"
        ) if exists else None
    finally:
        await conn.close()
    missed = published is None or published < required
    registry = CollectorRegistry()
    Gauge("features_publication_deadline_missed", "Required NYSE release missing", registry=registry).set(int(missed))
    Gauge("features_required_close_timestamp_seconds", "Required session close", registry=registry).set(required.timestamp())
    Gauge("features_watchdog_checked_timestamp_seconds", "Last successful ledger check", registry=registry).set(
        datetime.now(timezone.utc).timestamp())
    push_to_gateway(os.getenv("PUSHGATEWAY_URL", "http://localhost:9091"), job="publication-watchdog",
                    registry=registry, timeout=5)
    return {"deadline_missed": missed, "required_close": required.isoformat(),
            "published_close": published.isoformat() if published else None}
