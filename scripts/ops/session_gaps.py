"""Read-only, version-pinned Bronze gap audit. Prints JSON; never publishes data."""
import argparse
import json
import os
import pandas as pd
import pandas_market_calendars as mcal
import pyarrow as pa
import pyarrow.dataset as ds
from deltalake import DeltaTable
from dotenv import load_dotenv
from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("session_date")
    args = parser.parse_args()
    load_dotenv()
    schedule = mcal.get_calendar("NYSE").schedule(start_date=args.session_date, end_date=args.session_date)
    if schedule.empty:
        raise ValueError("Not an NYSE session")
    opening, close = schedule.iloc[0][["market_open", "market_close"]]
    path = os.environ["BRONZE_PATH"].replace("s3a://", "s3://")
    table = DeltaTable(path, storage_options={
        "AWS_ENDPOINT_URL": "http://localhost:8333", "AWS_ALLOW_HTTP": "true",
        "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1")})
    dataset = table.to_pyarrow_dataset()
    typ = dataset.schema.field("event_time").type
    bounds = (ds.field("event_time") >= pa.scalar(opening, type=typ)) & (ds.field("event_time") < pa.scalar(close, type=typ))
    if dataset.count_rows(filter=bounds) > 2_000_000:
        raise ValueError("Audit exceeds bounded local capacity")
    frame = dataset.to_table(columns=["symbol", "event_time"], filter=bounds).to_pandas()
    frame["event_time"] = pd.to_datetime(frame.event_time, utc=True)
    result = {"session": args.session_date, "bronze_path": path, "bronze_version": table.version(), "symbols": {}}
    expected = pd.date_range(opening, close, freq="30min", inclusive="left")
    for symbol in PRODUCT_SYMBOLS:
        times = frame.loc[frame.symbol == symbol, "event_time"].sort_values()
        observed = set(times.dt.floor("30min"))
        points = [opening, *times.tolist(), close]
        gaps = [{"start": a.isoformat(), "end": b.isoformat(), "seconds": (b-a).total_seconds()}
                for a,b in zip(points, points[1:]) if (b-a).total_seconds() > 120]
        result["symbols"][symbol] = {"rows": len(times), "observed_bars": len(observed), "expected_bars": len(expected),
            "first_trade": times.iloc[0].isoformat() if len(times) else None,
            "last_trade": times.iloc[-1].isoformat() if len(times) else None,
            "missing_bar_starts": [t.isoformat() for t in expected if t not in observed], "gaps_over_120s": gaps}
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
