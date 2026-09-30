"""Create a local-only synthetic feature release and train a tiny TFT champion.

This command exists to exercise development infrastructure when no licensed
historical feed is available. It refuses non-local PostgreSQL/MLflow targets and
marks every database row, run, and model version as synthetic development data.
"""

from __future__ import annotations

import argparse
import ipaddress
import os
import re
import uuid
import lightning.pytorch as pl
import mlflow
import mlflow.pytorch
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import psycopg2
import torch
from dotenv import load_dotenv
from lightning.pytorch.callbacks import EarlyStopping
from mlflow.tracking import MlflowClient
from pytorch_forecasting import QuantileLoss, TemporalFusionTransformer, TimeSeriesDataSet
from pytorch_forecasting.data import GroupNormalizer

from polyhorizon.core.product_symbols import PRODUCT_SYMBOLS
from polyhorizon.core.target_contract import attach_target_contract

ENCODER_LENGTH = 64
PREDICTION_LENGTH = 39
QUANTILES = [0.1, 0.5, 0.9]
SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _require_local(name: str, value: str, allowed: set[str]) -> None:
    if value not in allowed:
        raise RuntimeError(f"{name} must target a local development service, got {value!r}")


def _sql_identifier(name: str, value: str) -> str:
    if not SQL_IDENTIFIER.fullmatch(value):
        raise RuntimeError(f"{name} must be a safe PostgreSQL identifier")
    return value


def synthetic_frame() -> pd.DataFrame:
    calendar = mcal.get_calendar("NYSE")
    today = pd.Timestamp.now(tz="America/New_York").normalize()
    schedule = calendar.schedule(
        start_date=(today - pd.Timedelta(days=150)).date(),
        end_date=(today - pd.Timedelta(days=1)).date(),
    ).tail(85)
    timestamps = mcal.date_range(schedule, frequency="30min").tz_convert("UTC")
    if len(timestamps) < ENCODER_LENGTH + PREDICTION_LENGTH + 100:
        raise RuntimeError("Synthetic calendar did not produce enough training bars")

    frames: list[pd.DataFrame] = []
    base_prices = {"NVDA": 180.0, "AAPL": 245.0, "MSFT": 520.0}
    for symbol_index, symbol in enumerate(PRODUCT_SYMBOLS):
        rng = np.random.default_rng(20260923 + symbol_index)
        count = len(timestamps)
        cycle = np.sin(np.arange(count) / (17.0 + symbol_index * 2)) * 0.0012
        returns = 0.00008 + cycle + rng.normal(0, 0.0025, count)
        close = base_prices[symbol] * np.exp(np.cumsum(returns))
        open_price = close * (1 + rng.normal(0, 0.0008, count))
        spread = np.abs(rng.normal(0.0015, 0.0005, count))
        high = np.maximum(open_price, close) * (1 + spread)
        low = np.minimum(open_price, close) * (1 - spread)
        volume = rng.integers(300_000, 2_000_000, count, dtype=np.int64)
        frame = pd.DataFrame(
            {
                "symbol": symbol,
                "event_timestamp": timestamps,
                "window_start": timestamps - pd.Timedelta(minutes=30),
                "window_end": timestamps,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "total_volume": volume,
            }
        )
        frame["rolling_avg_close"] = frame.close.rolling(13, min_periods=1).mean()
        frame["rolling_volatility_close"] = (
            np.log(frame.close).diff().rolling(13, min_periods=2).std().fillna(0.0)
        )
        frames.append(frame)

    data = pd.concat(frames, ignore_index=True)
    ny_time = data.event_timestamp.dt.tz_convert("America/New_York")
    data["hour"] = data.event_timestamp.dt.hour.astype(int)
    data["minute"] = data.event_timestamp.dt.minute.astype(int)
    data["hour_sin"] = np.sin(2 * np.pi * data.event_timestamp.dt.hour / 24)
    data["hour_cos"] = np.cos(2 * np.pi * data.event_timestamp.dt.hour / 24)
    data["dow_sin"] = np.sin(2 * np.pi * data.event_timestamp.dt.dayofweek / 7)
    data["dow_cos"] = np.cos(2 * np.pi * data.event_timestamp.dt.dayofweek / 7)
    data["day_of_week"] = data.event_timestamp.dt.dayofweek.astype(str)
    data["is_holiday"] = "0.0"
    close_local = schedule.market_close.dt.tz_convert("America/New_York")
    early_dates = set(close_local[close_local.dt.hour < 16].index.date)
    data["is_early_close"] = ny_time.dt.date.isin(early_dates).astype(float).astype(str)
    data["log_volume"] = np.log1p(data.total_volume.astype(float))
    data["log_close"] = np.log(data.close)
    data["target"] = data.groupby("symbol", observed=True).log_close.diff().fillna(0.0)
    data["time_idx"] = data.groupby("symbol", observed=True).cumcount().astype(int)
    return data


def publish_features(data: pd.DataFrame) -> str:
    dataset_id = str(uuid.uuid4())
    db_host = os.environ.get("LOCAL_POSTGRES_HOST", "127.0.0.1")
    db_port = int(os.environ.get("LOCAL_POSTGRES_PORT", "5433"))
    address = ipaddress.ip_address(db_host)
    if not (address.is_loopback or address.is_private):
        raise RuntimeError("LOCAL_POSTGRES_HOST must be a loopback or private container address")
    db_schema = _sql_identifier("FEAST_DB_SCHEMA", os.environ.get("FEAST_DB_SCHEMA", "feast_schema"))
    offline_table = _sql_identifier(
        "OFFLINE_TABLE_NAME", os.environ.get("OFFLINE_TABLE_NAME", "stock_ohlcv")
    )
    snapshot_table = _sql_identifier(
        "SNAPSHOT_TABLE_NAME",
        os.environ.get("SNAPSHOT_TABLE_NAME", "stock_ohlcv_training_snapshot"),
    )
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=os.environ["POSTGRES_FEAST_DB"],
        user=os.environ["POSTGRES_FEAST_USER"],
        password=os.environ["POSTGRES_FEAST_PASSWORD"],
        connect_timeout=10,
    )
    columns = [
        "symbol", "event_timestamp", "window_start", "window_end", "open", "high", "low",
        "close", "total_volume", "rolling_avg_close", "rolling_volatility_close",
    ]
    try:
        with conn, conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {db_schema}")
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {db_schema}.{offline_table} (
                  symbol TEXT NOT NULL, event_timestamp TIMESTAMPTZ NOT NULL,
                  window_start TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL,
                  open DOUBLE PRECISION NOT NULL, high DOUBLE PRECISION NOT NULL,
                  low DOUBLE PRECISION NOT NULL, close DOUBLE PRECISION NOT NULL,
                  total_volume BIGINT NOT NULL, rolling_avg_close DOUBLE PRECISION NOT NULL,
                  rolling_volatility_close DOUBLE PRECISION NOT NULL,
                  dataset_version UUID NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  PRIMARY KEY (symbol, event_timestamp, dataset_version)
                )
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {db_schema}.{snapshot_table}
                (LIKE {db_schema}.{offline_table} INCLUDING ALL)
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {db_schema}.dataset_publications (
                  dataset_id UUID PRIMARY KEY, status TEXT NOT NULL,
                  source_kind TEXT NOT NULL, max_event_time TIMESTAMPTZ NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), metadata JSONB NOT NULL
                )
                """
            )
            rows = [tuple(row[col] for col in columns) + (dataset_id,) for _, row in data.iterrows()]
            placeholders = ",".join(["%s"] * (len(columns) + 1))
            insert = f"INSERT INTO {db_schema}.{offline_table} ({','.join(columns)},dataset_version) VALUES ({placeholders})"
            cursor.executemany(insert, rows)
            snapshot_insert = insert.replace(
                f"{db_schema}.{offline_table}", f"{db_schema}.{snapshot_table}", 1
            )
            cursor.executemany(snapshot_insert, rows)
            cursor.execute(
                f"""
                INSERT INTO {db_schema}.dataset_publications
                  (dataset_id, status, source_kind, max_event_time, metadata)
                VALUES (%s, 'published', 'synthetic-development', %s,
                        '{"synthetic": true, "production_eligible": false}'::jsonb)
                """,
                (dataset_id, data.event_timestamp.max().to_pydatetime()),
            )
    finally:
        conn.close()
    return dataset_id


def train_and_register(data: pd.DataFrame, dataset_id: str, epochs: int) -> str:
    training = TimeSeriesDataSet(
        data,
        time_idx="time_idx",
        target="target",
        group_ids=["symbol"],
        max_encoder_length=ENCODER_LENGTH,
        min_encoder_length=ENCODER_LENGTH,
        max_prediction_length=PREDICTION_LENGTH,
        min_prediction_length=PREDICTION_LENGTH,
        static_categoricals=["symbol"],
        time_varying_known_categoricals=["is_holiday", "is_early_close", "day_of_week"],
        time_varying_known_reals=["hour", "minute", "hour_sin", "hour_cos", "dow_sin", "dow_cos"],
        time_varying_unknown_reals=[
            "open", "high", "low", "close", "total_volume", "rolling_avg_close",
            "rolling_volatility_close", "log_volume", "log_close", "target",
        ],
        target_normalizer=GroupNormalizer(groups=["symbol"]),
        add_relative_time_idx=True,
        add_target_scales=True,
        add_encoder_length=True,
        allow_missing_timesteps=True,
    )
    loader = training.to_dataloader(train=True, batch_size=64, num_workers=0)
    model = TemporalFusionTransformer.from_dataset(
        training,
        learning_rate=0.01,
        hidden_size=8,
        attention_head_size=1,
        dropout=0.1,
        hidden_continuous_size=4,
        output_size=len(QUANTILES),
        loss=QuantileLoss(quantiles=QUANTILES),
        log_interval=-1,
        reduce_on_plateau_patience=2,
    )
    trainer = pl.Trainer(
        max_epochs=epochs,
        accelerator="cpu",
        devices=1,
        logger=False,
        enable_checkpointing=False,
        enable_model_summary=False,
        limit_train_batches=30,
        callbacks=[EarlyStopping(monitor="train_loss_epoch", patience=2, mode="min")],
        gradient_clip_val=0.5,
    )
    trainer.fit(model, train_dataloaders=loader)
    model.dataset_parameters = training.get_parameters()
    attach_target_contract(model, model.dataset_parameters)

    mlflow.set_tracking_uri("http://127.0.0.1:5001")
    mlflow.set_experiment("PolyHorizon_Local_Synthetic_Bootstrap")
    model_name = os.environ.get("MODEL_REGISTRY_NAME", "StockTFT_Quantile")
    with mlflow.start_run(run_name="synthetic-development-bootstrap") as run:
        mlflow.set_tags(
            {
                "data_provenance": "synthetic-development",
                "production_eligible": "false",
                "governance_qualification": "not-evaluated-synthetic",
                "dataset_version": dataset_id,
                "target_definition": "log(close_t / close_t_minus_1)",
                "return_to_price_method": "log",
            }
        )
        mlflow.log_params(
            {
                "encoder_length": ENCODER_LENGTH,
                "prediction_length": PREDICTION_LENGTH,
                "epochs": epochs,
                "rows": len(data),
                "synthetic": True,
            }
        )
        info = mlflow.pytorch.log_model(
            pytorch_model=model,
            artifact_path="tft_model",
            registered_model_name=model_name,
            pip_requirements=[
                "torch==2.10.0", "pytorch-forecasting==1.6.1", "lightning==2.6.1",
                "numpy", "pandas", "cloudpickle",
            ],
        )
        run_id = run.info.run_id

    client = MlflowClient()
    versions = [v for v in client.search_model_versions(f"name='{model_name}'") if v.run_id == run_id]
    if not versions:
        raise RuntimeError("MLflow did not create a registered model version")
    version = str(max(versions, key=lambda item: int(item.version)).version)
    for key, value in {
        "data_provenance": "synthetic-development",
        "production_eligible": "false",
        "governance_qualification": "not-evaluated-synthetic",
        "dataset_version": dataset_id,
        "forecast_horizon": str(PREDICTION_LENGTH),
        "target_definition": "log(close_t / close_t_minus_1)",
        "target_column": "target",
        "return_to_price_method": "log",
    }.items():
        client.set_model_version_tag(model_name, version, key, value)
    # Explicit local-development alias assignment. The normal governance flow
    # will never qualify or promote this synthetic version.
    client.set_registered_model_alias(model_name, "champion", version)
    client.set_registered_model_alias(model_name, "challenger", version)
    print(f"registered_model={model_name} version={version} run_id={run_id} artifact={info.model_uri}")
    return version


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--allow-synthetic", action="store_true")
    parser.add_argument("--epochs", type=int, default=2)
    args = parser.parse_args()
    if not args.allow_synthetic:
        raise SystemExit("Pass --allow-synthetic to acknowledge development-only data/model creation")
    if not 1 <= args.epochs <= 10:
        raise SystemExit("--epochs must be between 1 and 10")
    load_dotenv(".env", override=False)
    # The script deliberately ignores configured container/remote hosts and uses
    # only loopback endpoints below. This keeps synthetic writes local even when
    # `.env` contains Docker-network service names.
    os.environ["MLFLOW_TRACKING_URI"] = "http://127.0.0.1:5001"
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://127.0.0.1:8333"
    os.environ["MPLCONFIGDIR"] = "/tmp/polyhorizon-matplotlib"
    pl.seed_everything(20260923, workers=True)
    torch.set_float32_matmul_precision("medium")
    data = synthetic_frame()
    dataset_id = publish_features(data)
    version = train_and_register(data, dataset_id, args.epochs)
    print(
        f"synthetic_bootstrap_complete dataset={dataset_id} rows={len(data)} "
        f"symbols={','.join(PRODUCT_SYMBOLS)} model_version={version}"
    )


if __name__ == "__main__":
    main()
