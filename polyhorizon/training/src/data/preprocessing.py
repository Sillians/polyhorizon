import pandas as pd
import numpy as np
import mlflow.data
import pandas_market_calendars as mcal

from polyhorizon.training.utils.logger import get_logger
from polyhorizon.training.configs.settings import Config # check for errors


# The Modular Preprocessor Class (Adjust and edit accordingly after experimentation)
class TFTDataPreprocessor:
    def __init__(self, config: Config):
        self.config = config
        self.logger = get_logger("TFT_Preprocessor")
        self.nyse = mcal.get_calendar('NYSE')

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Main entry point for preprocessing."""
        self.logger.info("Starting preprocessing for %d rows", len(df))
        
        # 1. Track Raw Input Lineage
        raw_ds = mlflow.data.from_pandas(df, name="raw_database_snapshot")
        mlflow.log_input(raw_ds, context="preprocessing_input")


        from polyhorizon.core.product_symbols import filter_product_frame
        df = filter_product_frame(df)
        if df.empty:
            raise ValueError("No training rows match the product symbol allowlist")
        # Clean and Sort data
        df = self._clean_and_sort(df)
        # Add NYSE holiday / early close features (future-known for TFT decoder)
        df = self._add_nyse_holiday_features(df)
        # Add some time features
        df = self._add_time_features(df)
        # Log transforms (critical for stability)
        df = self._add_log_transforms(df)
        # forecast target
        df = self._calculate_multi_horizon_target(df, horizon=self.config.training.max_prediction_length)

        # 2. Track Processed Output Lineage
        processed_ds = mlflow.data.from_pandas(df, name="tft_ready_dataset")
        mlflow.log_input(processed_ds, context="preprocessing_output")
        
        mlflow.log_param("preprocessing_final_rows", len(df))
        return df

    def _clean_and_sort(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensures temporal consistency and removes duplicates."""
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)
        df["symbol"] = df["symbol"].astype("category")
        
        # Sort and create a continuous time index
        df = df.sort_values(["symbol", "event_timestamp"]).reset_index(drop=True)
        
        # IMPORTANT: time_idx must be an integer that increments linearly.
        # If your data is 30-min bars, each step is +1.
        df["time_idx"] = df.groupby("symbol", observed=True).cumcount()
        df["time_idx"] = df["time_idx"].astype(int)
        
        # Remove duplicates 
        initial_rows = len(df)
        df = df.drop_duplicates(subset=["symbol", "time_idx"], keep="last")
        df = df.drop_duplicates(subset=["symbol", "event_timestamp"], keep="last")
        self.logger.info("Dropped %d duplicate rows", initial_rows - len(df))
        return df


    # Add holiday features
    def _add_nyse_holiday_features(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        tz = 'America/New_York'
        
        # 1. Ensure event_timestamp is localized to NY to extract the correct date
        # This handles the case where a UTC timestamp on Monday 01:00 is actually Sunday night in NY
        ny_dt = df["event_timestamp"].dt.tz_convert(tz)
        df['date_key'] = ny_dt.dt.date
        
        # 2. Define Date Range for the Calendar
        # We use the actual range of the data to avoid unnecessary computation
        start_date = df['date_key'].min()
        end_date = df['date_key'].max()
        
        # 3. Get NYSE Schedule
        nyse = mcal.get_calendar('NYSE')
        schedule = nyse.schedule(start_date=start_date, end_date=end_date)
        
        # --- FIX: Timezone Conversion for Early Close ---
        # Convert schedule timestamps to NY time before checking the hour
        schedule['market_close_local'] = schedule['market_close'].dt.tz_convert(tz)
        
        # Early close is typically 13:00 ET. We check if hour is before 16:00 (4 PM)
        early_close_dates = schedule[schedule['market_close_local'].dt.hour < 16].index.date
        trading_dates = schedule.index.date

        # 4. Generate Flags
        # A day is a holiday if it's a weekday (Mon-Fri) but NOT in the trading schedule
        # We use ny_dt.dt.dayofweek < 5 to filter for weekdays
        is_weekday = ny_dt.dt.dayofweek < 5
        is_trading_day = df['date_key'].isin(trading_dates)
        
        df['is_holiday'] = (is_weekday & ~is_trading_day).astype(float)
        df['is_early_close'] = df['date_key'].isin(early_close_dates).astype(float)
        
        df["is_holiday"] = df["is_holiday"].astype(str)
        df["is_early_close"] = df["is_early_close"].astype(str)
        
        # Cleanup temporary column
        df = df.drop(columns=['date_key'])
        
        return df
    
    # Time features - use string categoricals for TFT
    def _add_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds cyclical encoding and market holidays."""
        ts = df["event_timestamp"]
        
        # Time features (hour/minute numeric, day_of_week categorical)
        df["hour"] = ts.dt.hour.astype(int)
        df["minute"] = ts.dt.minute.astype(int)
        df["day_of_week"] = ts.dt.dayofweek.astype(str)

        # Cyclical Sine/Cosine (Best practice for Deep Learning)
        df["hour_sin"] = np.sin(2 * np.pi * ts.dt.hour / 24)
        df["hour_cos"] = np.cos(2 * np.pi * ts.dt.hour / 24)
        df["dow_sin"] = np.sin(2 * np.pi * ts.dt.dayofweek / 7)
        df["dow_cos"] = np.cos(2 * np.pi * ts.dt.dayofweek / 7)
        
        return df

    # critical for stability
    def _add_log_transforms(self, df: pd.DataFrame) -> pd.DataFrame:
        if (df["close"] <= 0).any():
            raise ValueError("close must be strictly positive for log-return targets")
        df["log_volume"] = np.log1p(df["total_volume"])
        df["log_close"] = np.log(df["close"])
        return df

    def _calculate_multi_horizon_target(self, df: pd.DataFrame, horizon: int) -> pd.DataFrame:
        """
        Calculates the forecast target. 
        For a 3-day forecast at 30m intervals, HORIZON = 39 (13 bars/day * 3).
        """
        
        # # Shift per symbol to avoid data leakage between tickers
        # df["future_log_close"] = df.groupby("symbol", observed=True)["log_close"].shift(-horizon)
        
        # # Target is the log return over the horizon
        # df["target"] = df["future_log_close"] - df["log_close"]
        
        # # Drop rows where target is NaN (the very end of the series)
        # return df.dropna(subset=["target"]).reset_index(drop=True)
        
        # Target: Use 1-step log returns (Stationary)
        # The TFT will learn to predict a sequence of these 1-step returns
        # to fulfill your max_prediction_length.
        # The first observation of each symbol has no return target. Keeping a
        # synthetic zero here would teach the model a value that never existed.
        df["target"] = df.groupby("symbol", observed=True)["log_close"].diff()
        
        # Drop rows where we can't compute the target or target is NaN (the very end of the series)
        df = df.dropna(subset=["target"]).reset_index(drop=True)
        return df
    
