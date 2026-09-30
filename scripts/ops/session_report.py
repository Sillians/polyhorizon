"""Read-only daily evidence report; never publishes, trains or promotes."""
import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from urllib.parse import urlsplit

import boto3
from botocore.config import Config
from deltalake import DeltaTable
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from polyhorizon.core.dataset_release import DatasetRelease
from polyhorizon.core.session_qualification import qualify_session, qualify_trades
from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
from polyhorizon.streaming.src.market_schedule import get_market_session
from zoneinfo import ZoneInfo


def options():
    return {"AWS_ENDPOINT_URL": "http://localhost:8333", "AWS_ALLOW_HTTP": "true",
            "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
            "AWS_REGION": os.getenv("AWS_REGION", "us-east-1")}


def table(path, version=None):
    return DeltaTable(path.replace("s3a://", "s3://"), version=version, storage_options=options())


def bounded_frame(delta, opening, close, field, columns=None):
    data = delta.to_pyarrow_dataset()
    typ = data.schema.field(field).type
    bounds = (ds.field(field) >= pa.scalar(opening, type=typ)) & (ds.field(field) < pa.scalar(close, type=typ))
    if data.count_rows(filter=bounds) > 2_000_000:
        raise ValueError("Session audit exceeds two million rows")
    return data.to_table(columns=columns, filter=bounds).to_pandas()


def probe():
    bronze = table(os.environ["BRONZE_PATH"])
    # Commit progress is version-based, not file modification time or wall-clock age.
    now = datetime.now(timezone.utc)
    recent = bounded_frame(bronze, now - timedelta(minutes=10), now, "event_time", ["event_time"])
    age = float((pd.Timestamp(now) - pd.to_datetime(recent.event_time, utc=True).max()).total_seconds()) if len(recent) else 600.0
    return {"bronze_version": bronze.version(), "event_age_seconds": age}


def manifests(day):
    root = urlsplit(os.environ["FEATURE_OUTPUT_PATH"].replace("s3a://", "s3://"))
    client = boto3.client("s3", endpoint_url="http://localhost:8333",
                          config=Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 2}))
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=root.netloc,
                Prefix=root.path.strip("/") + "/manifests/", PaginationConfig={"MaxItems": 10000})
    results = []
    for page in pages:
        if page.get("NextToken"):
            raise ValueError("Manifest discovery exceeds bounded capacity")
        for item in page.get("Contents", []):
            if not item["Key"].rsplit("/", 1)[-1].startswith("part-"):
                continue
            if item["Size"] > 1_000_000:
                raise ValueError("Manifest exceeds size bound")
            body = client.get_object(Bucket=root.netloc, Key=item["Key"])["Body"].read()
            release = DatasetRelease.model_validate_json(body)
            if str(release.market_close.astimezone(ZoneInfo("America/New_York")).date()) == day:
                release.validate_root(os.environ["FEATURE_OUTPUT_PATH"])
                results.append(release)
    return sorted(results, key=lambda r: r.completed_at)


def gap_details(frame, opening, close):
    result = {}
    for symbol in PRODUCT_SYMBOLS:
        times = pd.to_datetime(frame.loc[frame.symbol == symbol, "event_time"], utc=True).sort_values()
        points = [opening, *times.tolist(), close]
        gaps = [{"start": a.isoformat(), "end": b.isoformat(), "seconds": (b-a).total_seconds()}
                for a, b in zip(points, points[1:]) if (b-a).total_seconds() > 120]
        expected = pd.date_range(opening, close, freq="30min", inclusive="left")
        observed = set(times.dt.floor("30min"))
        result[symbol] = {"rows": len(times), "gaps_over_120s": gaps,
                          "missing_bars": [t.isoformat() for t in expected if t not in observed]}
    return result


def report(day):
    opening, close = get_market_session(datetime.fromisoformat(day).replace(tzinfo=ZoneInfo("America/New_York")), "NYSE", "America/New_York")
    result = {"session": day, "status": "incomplete", "published": False,
              "trained": False, "promoted": False, "errors": []}
    bronze = table(os.environ["BRONZE_PATH"])
    frame = bounded_frame(bronze, opening, close, "event_time", ["symbol", "event_time", "price", "volume"])
    result["bronze_version"] = bronze.version()
    result["symbols"] = gap_details(frame, opening, close)
    candidates = manifests(day)
    if not candidates:
        result["errors"].append("No completed, qualified dataset manifest for this session")
        return result
    release = candidates[-1]
    pinned = bounded_frame(table(release.bronze_path, release.bronze_version), opening, close, "event_time",
                           ["symbol", "event_time", "price", "volume"])
    gold = bounded_frame(table(release.gold_path, release.gold_version), opening, close, "window_start")
    try:
        evidence = qualify_session(gold, close)
        evidence["trade_coverage"] = qualify_trades(pinned, close)
        if evidence != release.session_qualification:
            raise ValueError("Qualification evidence differs from persisted manifest")
        result.update(status="qualified", evidence=evidence, dataset_release=release.model_dump(mode="json"))
    except ValueError as exc:
        result["errors"].append(str(exc))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("day", nargs="?")
    parser.add_argument("--probe", action="store_true")
    args = parser.parse_args()
    load_dotenv()
    try:
        value = probe() if args.probe else report(args.day)
    except Exception as exc:
        # Never put raw connector error strings/credentials in reports.
        value = {"status": "operational_failure", "error_type": type(exc).__name__,
                 "session": args.day, "published": False, "trained": False, "promoted": False}
        if args.probe:
            raise SystemExit(1)
    value["checked_at"] = datetime.now(timezone.utc).isoformat() if not args.probe else None
    print(json.dumps(value, indent=2, default=str))


if __name__ == "__main__":
    main()
