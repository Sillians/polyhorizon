"""Resumable historical bar collection and immutable, training-only releases."""
import hashlib
import json
import time
import os
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pandas as pd
import pandas_market_calendars as mcal
import pyarrow as pa
import requests
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError
from prefect import flow

from polyhorizon.core.dataset_release import DatasetRelease, canonical_path, validate_feature_frame
from polyhorizon.core.historical_qualification import HistoricalSource, bars_digest, qualify_historical_bars
from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS


def save_json_exclusive(path: Path, payload):
    """Crash-safe cache/manifest write, without overwriting existing evidence."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as output:
        temporary = Path(output.name)
        try:
            json.dump(payload, output, allow_nan=False, sort_keys=True, indent=2)
            output.flush()
            os.fsync(output.fileno())
            os.link(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def fetch_finnhub(start: date, end: date, token: str, cache: Path, session=None):
    if not token:
        raise ValueError("FINNHUB_TOKEN is required")
    cache.mkdir(parents=True, exist_ok=True)
    session = session or requests.Session()
    schedule = mcal.get_calendar("NYSE").schedule(start_date=start, end_date=end)
    frames, retrievals = [], []
    for _, day in schedule.iterrows():
        if day.market_close > pd.Timestamp.now(tz="UTC"):
            raise ValueError("Backfill refuses unfinished sessions")
        for symbol in PRODUCT_SYMBOLS:
            params = {"symbol": symbol, "resolution": "30", "from": int(day.market_open.timestamp()),
                      "to": int(day.market_close.timestamp()) - 1}
            identity = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()
            path = cache / f"{identity}.json"
            if not path.exists():
                for attempt in range(5):
                    try:
                        response = session.get("https://finnhub.io/api/v1/stock/candle", params=params,
                                               headers={"X-Finnhub-Token": token}, timeout=(10, 60))
                    except requests.RequestException:
                        if attempt == 4:
                            raise RuntimeError("Historical provider connection retry budget exhausted") from None
                        time.sleep(min(60, 2 ** (attempt + 1)))
                        continue
                    if response.status_code == 429 or response.status_code >= 500:
                        if attempt == 4:
                            raise RuntimeError("Historical provider retry budget exhausted")
                        time.sleep(min(60, 2 ** (attempt + 1)))
                        continue
                    # Do not include response URLs or credentials in provider errors.
                    if response.status_code != 200:
                        raise RuntimeError(f"Historical provider denied request: HTTP {response.status_code}")
                    payload = response.json()
                    if payload.get("s") != "ok":
                        raise ValueError(f"No historical candles for {symbol} on {day.market_open.date()}")
                    record = {"request": params, "retrieved_at": datetime.now(timezone.utc).isoformat(), "payload": payload}
                    try:
                        save_json_exclusive(path, record)
                    except FileExistsError:
                        pass
                    time.sleep(1)  # Bounded sequential requests; no parallel quota burst.
                    break
            record = json.loads(path.read_text())
            if record["request"] != params:
                raise ValueError("Backfill cache request identity mismatch")
            payload = record["payload"]
            arrays = [payload.get(k, []) for k in ("t", "o", "h", "l", "c", "v")]
            if not arrays[0] or len({len(a) for a in arrays}) != 1:
                raise ValueError("Historical provider returned malformed candle arrays")
            frame = pd.DataFrame(dict(zip(["window_start", "open", "high", "low", "close", "total_volume"], arrays)))
            frame["window_start"] = pd.to_datetime(frame.window_start, unit="s", utc=True)
            frame = frame.loc[(frame.window_start >= day.market_open) & (frame.window_start < day.market_close)].copy()
            frame["window_end"] = frame.window_start + pd.Timedelta(minutes=30)
            frame["symbol"] = symbol
            frames.append(frame)
            retrievals.append(pd.Timestamp(record["retrieved_at"]))
    if not frames:
        raise ValueError("No historical sessions requested")
    return pd.concat(frames, ignore_index=True), max(retrievals).to_pydatetime()


def compute_historical_features(bars, lookback_bars):
    bars = bars.copy().sort_values(["symbol", "window_start"]).reset_index(drop=True)
    for column in ("window_start", "window_end"):
        bars[column] = pd.to_datetime(bars[column], utc=True)
    for column in ("open", "high", "low", "close"):
        bars[column] = bars[column].astype(float)
    bars["total_volume"] = bars.total_volume.astype("int64")
    features = bars.copy()
    groups = features.groupby("symbol", sort=False).close
    features["rolling_avg_close"] = groups.transform(lambda c: c.rolling(lookback_bars + 1, min_periods=1).mean())
    features["rolling_volatility_close"] = groups.transform(lambda c: c.rolling(lookback_bars + 1, min_periods=2).std(ddof=1))
    features = features.dropna(subset=["rolling_volatility_close"]).copy()
    features["event_timestamp"] = features.window_end
    return features


def build_release(bars, source: HistoricalSource, feature_root: str, raw_root: str,
                  storage_options=None, lookback_bars=60, image_tag="local"):
    if lookback_bars < 1:
        raise ValueError("lookback_bars must be positive")
    evidence = qualify_historical_bars(bars, source)
    bars = bars.copy().sort_values(["symbol", "window_start"]).reset_index(drop=True)
    for column in ("window_start", "window_end"):
        bars[column] = pd.to_datetime(bars[column], utc=True)
    for column in ("open", "high", "low", "close"):
        bars[column] = bars[column].astype(float)
    bars["total_volume"] = bars.total_volume.astype("int64")
    identity = json.dumps({"source": source.model_dump(mode="json"), "lookback": lookback_bars,
                           "definition": "ohlcv-rolling-v1"}, sort_keys=True)
    dataset_id = uuid5(NAMESPACE_URL, identity)
    raw_path = f"{canonical_path(raw_root)}/{dataset_id}"
    feature_path = f"{canonical_path(feature_root)}/datasets/{dataset_id}"
    features = compute_historical_features(bars, lookback_bars)
    close = bars.window_end.max().to_pydatetime()
    release = DatasetRelease(dataset_id=dataset_id, bronze_path=raw_path, bronze_version=0,
        gold_path=raw_path, gold_version=0, feature_path=feature_path, feature_version=0,
        frequency="30 minutes", lookback_bars=lookback_bars, image_tag=image_tag,
        market_close=close, completed_at=source.retrieved_at,
        min_event_time=features.event_timestamp.min().to_pydatetime(), max_event_time=close,
        row_count=len(features), historical_source=source, session_qualification=evidence)
    validate_feature_frame(features, release)
    for path, frame in ((raw_path, bars), (feature_path, features)):
        try:
            previous = DeltaTable(path, storage_options=storage_options)
        except TableNotFoundError:
            write_deltalake(path, pa.Table.from_pandas(frame, preserve_index=False), mode="error",
                           storage_options=storage_options)
        else:
            if previous.version() != 0:
                raise ValueError("Immutable historical release was modified")
            pd.testing.assert_frame_equal(previous.to_pandas().reset_index(drop=True),
                                          frame.reset_index(drop=True), check_dtype=False, check_exact=True)
    manifest = release.model_dump(mode="json")
    if feature_path.startswith("s3://"):
        import boto3
        from botocore.exceptions import ClientError
        from urllib.parse import urlsplit
        options = storage_options or {}
        client = boto3.client("s3", endpoint_url=options.get("AWS_ENDPOINT_URL"),
            aws_access_key_id=options.get("AWS_ACCESS_KEY_ID"), aws_secret_access_key=options.get("AWS_SECRET_ACCESS_KEY"),
            region_name=options.get("AWS_REGION", "us-east-1"))
        parsed = urlsplit(feature_path)
        key = parsed.path.lstrip("/") + "/manifest.json"
        try:
            existing = json.loads(client.get_object(Bucket=parsed.netloc, Key=key)["Body"].read())
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in {"NoSuchKey", "404"}:
                raise
            try:
                client.put_object(Bucket=parsed.netloc, Key=key, Body=json.dumps(manifest, sort_keys=True).encode(),
                                  ContentType="application/json", IfNoneMatch="*")
            except ClientError as conflict:
                if conflict.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
                    raise
                existing = json.loads(client.get_object(Bucket=parsed.netloc, Key=key)["Body"].read())
                if existing != manifest:
                    raise ValueError("Stored historical manifest differs") from None
        else:
            if existing != manifest:
                raise ValueError("Stored historical manifest differs")
    else:
        path = Path(feature_path) / "manifest.json"
        try:
            save_json_exclusive(path, manifest)
        except FileExistsError:
            if json.loads(path.read_text()) != manifest:
                raise ValueError("Stored historical manifest differs") from None
    return release


@flow(name="historical-backfill", retries=0)
def historical_backfill_flow(start_date: str, end_date: str, adjustment: str,
                             feature_root: str, raw_root: str, cache_dir: str,
                             csv_path: str | None = None):
    from dotenv import load_dotenv
    load_dotenv()
    start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
    if start > end or (end - start).days > 730:
        raise ValueError("Backfill range must be ordered and no longer than 730 days")
    if csv_path:
        bars = pd.read_csv(csv_path)
        retrieved = datetime.fromtimestamp(Path(csv_path).stat().st_mtime, tz=timezone.utc)
        provider = "csv"
    else:
        bars, retrieved = fetch_finnhub(start, end, os.getenv("FINNHUB_TOKEN", ""), Path(cache_dir))
        provider = "finnhub"
    request = json.dumps({"provider": provider, "start": start_date, "end": end_date,
                          "adjustment": adjustment, "symbols": list(PRODUCT_SYMBOLS)}, sort_keys=True)
    source = HistoricalSource(provider=provider, adjustment=adjustment, start_date=start,
        end_date=end, retrieved_at=retrieved, content_sha256=bars_digest(bars),
        request_sha256=hashlib.sha256(request.encode()).hexdigest())
    from polyhorizon.features.configs.settings import load_config
    bucket = load_config().bucket_details
    options = {"AWS_ENDPOINT_URL": str(bucket.seaweedfs_s3_endpoint),
               "AWS_ACCESS_KEY_ID": bucket.seaweedfs_access_key, "AWS_SECRET_ACCESS_KEY": bucket.seaweedfs_secret_key,
               "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true", "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false"}
    release = build_release(bars, source, feature_root, raw_root, options,
                            image_tag=os.getenv("IMAGE_TAG", "local"))
    # No publication, training or alias movement occurs merely by collecting bars.
    return release.model_dump(mode="json")
