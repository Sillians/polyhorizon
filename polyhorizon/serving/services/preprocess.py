from __future__ import annotations

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal

from polyhorizon.serving.configs.settings import Config
from polyhorizon.serving.utils.logger import get_logger


class ServingPreprocessor:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.logger = get_logger("ServingPreprocessor")
        self.nyse = mcal.get_calendar("NYSE")

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        self._validate_base_columns(df)
        df = self._clean_and_sort(df)
        df = self._add_nyse_holiday_features(df)
        df = self._add_time_features(df)
        df = self._add_log_transforms(df)
        df = self._add_target_returns(df)
        return df

    def build_prediction_frame(self, history: pd.DataFrame) -> pd.DataFrame:
        """Append a real future NYSE decoder window to an encoder history."""
        if history.empty:
            raise ValueError("Cannot build prediction frame from empty history")

        time_field = self.config.inference.time_field
        prediction_length = self.config.inference.max_prediction_length
        history = history.copy()
        history[time_field] = pd.to_datetime(history[time_field], utc=True, errors="coerce")
        history = history.dropna(subset=[time_field]).sort_values(time_field)
        if history.empty:
            raise ValueError("Feature history contains no valid timestamps")

        future_times = self._future_market_timestamps(
            history[time_field].iloc[-1],
            prediction_length,
        )
        last_row = history.iloc[-1]
        future = pd.DataFrame([last_row.to_dict() for _ in future_times])
        future[time_field] = future_times

        # Unknown covariates use the last observed value as a neutral decoder
        # placeholder. TFT masks future targets; setting them explicitly keeps
        # TimeSeriesDataSet validation deterministic.
        target = self.config.inference.target
        future[target] = 0.0
        combined = pd.concat([history, future], ignore_index=True)
        return self.enrich(combined)

    def _future_market_timestamps(self, latest: pd.Timestamp, periods: int) -> pd.DatetimeIndex:
        latest = pd.Timestamp(latest)
        if latest.tzinfo is None:
            latest = latest.tz_localize("UTC")
        else:
            latest = latest.tz_convert("UTC")

        # Expand the calendar window until it covers long horizons and holiday
        # clusters. pandas_market_calendars produces only timestamps belonging
        # to actual NYSE sessions, including early closes.
        calendar_days = max(14, periods * 2)
        for _ in range(6):
            schedule = self.nyse.schedule(
                start_date=latest.date(),
                end_date=(latest + pd.Timedelta(days=calendar_days)).date(),
            )
            candidates = mcal.date_range(schedule, frequency=self.config.inference.freq)
            candidates = pd.DatetimeIndex(candidates).tz_convert("UTC")
            candidates = candidates[candidates > latest]
            if len(candidates) >= periods:
                return candidates[:periods]
            calendar_days *= 2
        raise ValueError(f"Unable to construct {periods} future NYSE timestamps")

    def _validate_base_columns(self, df: pd.DataFrame) -> None:
        required = {"symbol", self.config.inference.time_field, "open", "high", "low", "close", "total_volume"}
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required base columns for serving transforms: {missing}")

    def _clean_and_sort(self, df: pd.DataFrame) -> pd.DataFrame:
        time_field = self.config.inference.time_field
        group_field = self.config.inference.group_id_field
        if time_field not in df.columns:
            raise ValueError(f"Missing time field '{time_field}' in features")

        df[time_field] = pd.to_datetime(df[time_field], utc=True, errors="coerce")
        df = df.dropna(subset=[time_field])
        if group_field in df.columns:
            df[group_field] = df[group_field].astype("category")
            df = df.sort_values([group_field, time_field]).reset_index(drop=True)
            df[self.config.inference.time_idx_field] = (
                df.groupby(group_field, observed=True).cumcount().astype(int)
            )
        else:
            df = df.sort_values(time_field).reset_index(drop=True)
            df[self.config.inference.time_idx_field] = np.arange(len(df), dtype=int)

        df = df.drop_duplicates(
            subset=[group_field, self.config.inference.time_idx_field], keep="last"
        )
        df = df.drop_duplicates(subset=[group_field, time_field], keep="last")
        return df

    def _add_nyse_holiday_features(self, df: pd.DataFrame) -> pd.DataFrame:
        time_field = self.config.inference.time_field
        tz = "America/New_York"
        ny_dt = df[time_field].dt.tz_convert(tz)
        df["date_key"] = ny_dt.dt.date

        start_date = df["date_key"].min()
        end_date = df["date_key"].max()
        schedule = self.nyse.schedule(start_date=start_date, end_date=end_date)
        schedule["market_close_local"] = schedule["market_close"].dt.tz_convert(tz)

        early_close_dates = schedule[schedule["market_close_local"].dt.hour < 16].index.date
        trading_dates = schedule.index.date

        is_weekday = ny_dt.dt.dayofweek < 5
        is_trading_day = df["date_key"].isin(trading_dates)
        df["is_holiday"] = (is_weekday & ~is_trading_day).astype(float)
        df["is_early_close"] = df["date_key"].isin(early_close_dates).astype(float)

        df["is_holiday"] = df["is_holiday"].astype(str)
        df["is_early_close"] = df["is_early_close"].astype(str)

        return df.drop(columns=["date_key"])

    def _add_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        time_field = self.config.inference.time_field
        ts = df[time_field]
        df["hour"] = ts.dt.hour.astype(int)
        df["minute"] = ts.dt.minute.astype(int)
        df["day_of_week"] = ts.dt.dayofweek.astype(str)

        df["hour_sin"] = np.sin(2 * np.pi * ts.dt.hour / 24)
        df["hour_cos"] = np.cos(2 * np.pi * ts.dt.hour / 24)
        df["dow_sin"] = np.sin(2 * np.pi * ts.dt.dayofweek / 7)
        df["dow_cos"] = np.cos(2 * np.pi * ts.dt.dayofweek / 7)
        return df

    def _add_log_transforms(self, df: pd.DataFrame) -> pd.DataFrame:
        if (df["close"] <= 0).any():
            raise ValueError("close must be strictly positive for log-return inference")
        if "total_volume" in df.columns and "log_volume" not in df.columns:
            df["log_volume"] = np.log1p(df["total_volume"].astype(float))
        if "close" in df.columns and "log_close" not in df.columns:
            df["log_close"] = np.log(df["close"].astype(float))
        return df

    def _add_target_returns(self, df: pd.DataFrame) -> pd.DataFrame:
        target_field = self.config.inference.target
        group_field = self.config.inference.group_id_field
        if "log_close" not in df.columns:
            return df
        calculated = df.groupby(group_field, observed=True)["log_close"].diff().fillna(0.0)
        if target_field not in df.columns:
            df[target_field] = calculated
        else:
            df[target_field] = pd.to_numeric(df[target_field], errors="coerce").fillna(calculated)
        return df
