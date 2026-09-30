"""Bounded Kafka recovery into a new LOCAL Delta table, never production paths.

No consumer group, committed offsets, Spark checkpoints or feature publication.
Run with PYTHONPATH=. .venv/bin/python scripts/ops/recover_kafka.py --output /tmp/recovery
"""
import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import time
from uuid import uuid4

from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS


def validate_trade(value):
    if not isinstance(value, dict) or value.get("symbol") not in PRODUCT_SYMBOLS:
        raise ValueError("unsupported_symbol_or_shape")
    for name in ("price", "volume"):
        number = value.get(name)
        if isinstance(number, bool) or not isinstance(number, (int, float)) or not math.isfinite(number) or number <= 0:
            raise ValueError(f"invalid_{name}")
    ts = value.get("timestamp")
    if isinstance(ts, bool) or not isinstance(ts, int) or not 10**12 <= ts < 10**14:
        raise ValueError("invalid_epoch_milliseconds")
    return {"symbol": value["symbol"], "price": float(value["price"]),
            "volume": float(value["volume"]), "timestamp": ts,
            "event_time": datetime.fromtimestamp(ts / 1000, timezone.utc)}


def coverage(rows):
    import pandas as pd
    import pandas_market_calendars as mcal
    result = {}
    for symbol in PRODUCT_SYMBOLS:
        times = sorted(r["event_time"] for r in rows if r["symbol"] == symbol)
        sessions = []
        if times:
            schedule = mcal.get_calendar("NYSE").schedule(
                start_date=times[0].date(), end_date=times[-1].date())
            for day, session in schedule.iterrows():
                opened, closed = session.market_open, session.market_close
                bars = pd.date_range(opened, closed, freq="30min", inclusive="left")
                seen = {int((pd.Timestamp(t) - opened).total_seconds() // 1800)
                        for t in times if opened <= t < closed}
                sessions.append({"date": str(day.date()), "expected_bars": len(bars),
                                 "observed_bars": len(seen),
                                 "missing_bars": [b.isoformat() for i, b in enumerate(bars) if i not in seen]})
        result[symbol] = {"rows": len(times), "min": times[0].isoformat() if times else None,
                          "max": times[-1].isoformat() if times else None, "sessions": sessions}
    return result


def recover(servers, topic, output, max_records=1_000_000, timeout=120):
    from kafka import KafkaConsumer, TopicPartition
    from deltalake import write_deltalake, DeltaTable
    import pyarrow as pa
    root = Path(output).resolve() / str(uuid4())
    root.mkdir(parents=True, exist_ok=False)
    c = KafkaConsumer(bootstrap_servers=servers.split(","), group_id=None,
                      enable_auto_commit=False, request_timeout_ms=15000,
                      api_version_auto_timeout_ms=10000)
    try:
        partitions = c.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError("Topic has no partitions")
        tps = [TopicPartition(topic, p) for p in sorted(partitions)]
        c.assign(tps)
        starts, ends = c.beginning_offsets(tps), c.end_offsets(tps)
        if sum(ends[t] - starts[t] for t in tps) > max_records:
            raise RuntimeError("Recovery exceeds record limit; partition the recovery explicitly")
        bounds = {str(t.partition): {"start": starts[t], "end_exclusive": ends[t]} for t in tps}
        (root / "bounds.json").write_text(json.dumps({"topic": topic, "partitions": bounds}, indent=2))
        for t in tps:
            c.seek(t, starts[t])
        rows, rejected, duplicates, seen = [], 0, 0, set()
        deadline = time.monotonic() + timeout
        with (root / "rejected.jsonl").open("x") as quarantine:
            while any(c.position(t) < ends[t] for t in tps):
                if time.monotonic() >= deadline:
                    raise RuntimeError("Recovery timed out before captured end offsets; no success report emitted")
                if any(c.beginning_offsets(tps)[t] > starts[t] for t in tps):
                    raise RuntimeError("Kafka retention advanced during recovery")
                for tp, records in c.poll(timeout_ms=1000, max_records=2000).items():
                    for record in records:
                        if record.offset >= ends[tp]:
                            continue
                        try:
                            row = validate_trade(json.loads(record.value))
                        except (ValueError, TypeError, OverflowError, OSError) as exc:
                            rejected += 1
                            quarantine.write(json.dumps({"partition": tp.partition, "offset": record.offset,
                                "reason": type(exc).__name__, "payload_base64": base64.b64encode(record.value).decode()}) + "\n")
                            continue
                        fingerprint = (row["symbol"], row["timestamp"], row["price"], row["volume"])
                        duplicates += fingerprint in seen
                        seen.add(fingerprint)
                        row.update(kafka_topic=topic, kafka_partition=tp.partition, kafka_offset=record.offset)
                        rows.append(row)
        expected = sum(ends[t] - starts[t] for t in tps)
        if len(rows) + rejected != expected:
            raise RuntimeError("Offset range contains missing records; inspect retention/compaction")
        if rows:
            write_deltalake(str(root / "bronze"), pa.Table.from_pylist(rows), mode="error")
            assert DeltaTable(str(root / "bronze")).to_pyarrow_table().num_rows == len(rows)
        report = {"status": "recovered_isolated", "production_eligible": False,
                  "reason": "Recovery is not a completed feature release", "partitions": bounds,
                  "valid_rows": len(rows), "rejected_rows": rejected,
                  "identical_trade_payloads": duplicates,
                  "duplicate_policy": "Retain: identical trades are not proven duplicates without provider trade IDs",
                  "coverage": coverage(rows), "delta_path": str(root / "bronze") if rows else None,
                  "bounds_sha256": hashlib.sha256((root / "bounds.json").read_bytes()).hexdigest()}
        (root / "report.json").write_text(json.dumps(report, indent=2))
        print(root / "report.json")
        return report
    finally:
        c.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--servers", default="localhost:9092,localhost:9093,localhost:9094")
    parser.add_argument("--topic", default="stock-trades")
    parser.add_argument("--output", required=True)
    parser.add_argument("--max-records", type=int, default=1_000_000)
    args = parser.parse_args()
    recover(args.servers, args.topic, args.output, args.max_records)
