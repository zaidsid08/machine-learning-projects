"""
build_features.py

Create model-ready, time-series features from the cleaned traffic dataset.

Expected input schema (default):
    - ID (optional but recommended)
    - DateTime (timestamp)
    - Junction (segment/location id)
    - Vehicles (target / traffic signal)

Outputs:
    - A feature dataframe suitable for ML training (e.g., one-step-ahead forecasting)
"""

from __future__ import annotations


from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from pathlib import Path


@dataclass(frozen=True)
class FeatureConfig:
    """
    Configuration for feature generation.

    Attributes:
        segment_column: Column that identifies the traffic segment (e.g., Junction).
        time_column: Timestamp column (must be parseable to datetime).
        target_column: Column containing the traffic signal to predict (e.g., Vehicles).
        id_column: Optional unique row identifier column (e.g., ID). Keep for traceability.
        lags: Which lag steps to generate (e.g., [1, 2, 3] means t-1, t-2, t-3).
        rolling_windows: Window sizes (in rows) for rolling statistics (e.g., [3, 6, 12]).
        horizon: Forecast horizon in steps (1 = predict next timestamp).
        dropna_after_features: Whether to drop rows with NaNs created by lag/rolling features.
    """
    segment_column: str = "Junction"
    time_column: str = "DateTime"
    target_column: str = "Vehicles"
    id_column: Optional[str] = "ID"

    lags: tuple[int, ...] = (1, 2, 3)
    rolling_windows: tuple[int, ...] = (3, 6, 12)
    horizon: int = 1

    dropna_after_features: bool = True


def load_processed_timeseries(path: Path) -> pd.DataFrame:
    """
    Load the cleaned, processed time-series CSV.

    Args:
        path: Path to processed CSV (e.g., data/processed/segment_timeseries.csv)

    Returns:
        DataFrame with at least (DateTime, Junction, Vehicles), optionally ID.
    """
    if not path.exists():
        raise FileNotFoundError(f"Processed traffic data file not found: {path}")

    try:
        df = pd.read_csv(path)
    except Exception as e:
        raise ValueError(f"Failed to read CSV file: {e}")

    if df.empty:
        raise ValueError("Processed traffic CSV is empty.")

    return df

def ensure_datetime(df: pd.DataFrame, time_column: str) -> pd.DataFrame:
    """
    Ensure the timestamp column is datetime dtype.

    Args:
        df: Input dataframe.
        time_column: Column name containing timestamps.

    Returns:
        DataFrame with time_column converted to datetime dtype.

    Raises:
        ValueError: If any timestamps cannot be parsed.
    """
    new_df = df.copy()
    new_df[time_column] = pd.to_datetime(new_df[time_column], errors="raise")
    return new_df

def sort_by_segment_and_time(df: pd.DataFrame, segment_column: str, time_column: str) -> pd.DataFrame:
    """
    Sort the dataframe by (segment_column, time_column).

    Args:
        df: Input dataframe.
        segment_column: Segment identifier column.
        time_column: Timestamp column.

    Returns:
        Sorted dataframe.
    """
    df = df.sort_values(by=segment_column).reset_index(drop=True)
    rows = [df.iloc[[i]] for i in range(len(df))]
    timestamp_rows = []
    current = rows[0][segment_column].iloc[0]
    previous_index = 0
    first = True
    for index, row in enumerate(rows):
        if row[segment_column].iloc[0] != current:
            if first:
                subset = rows[:index]
                sorted_rows = sorted(subset,key=lambda r: pd.to_datetime(r[time_column].iloc[0]))
                timestamp_rows.extend(sorted_rows)
                previous_index = index

            else:
                subset = rows[previous_index:index]
                sorted_rows = sorted(subset,key=lambda r: pd.to_datetime(r[time_column].iloc[0]))
                timestamp_rows.extend(sorted_rows)
                previous_index = index


            current = rows[index][segment_column].iloc[0]
        first = False

    subset = rows[previous_index:]
    sorted_rows = sorted(subset,key=lambda r: pd.to_datetime(r[time_column].iloc[0]))
    timestamp_rows.extend(sorted_rows)

    return pd.concat(timestamp_rows, ignore_index=True)


def add_time_features(df: pd.DataFrame, time_column: str) -> pd.DataFrame:
    """
    Add time-based features derived from the timestamp.

    Typical features:
        - hour (0-23)
        - day_of_week (0=Mon..6=Sun)
        - is_weekend (0/1)

    Args:
        df: Input dataframe with a datetime time_column.
        time_column: Timestamp column.

    Returns:
        DataFrame with new time feature columns added.
    """
    # new_df = df.copy()
    # new_df["hour"] = []
    # new_df["day_of_week"] = []
    # new_df["is_weekend"] = []
    # i = 0
    # for dt in new_df[time_column]:
    #     new_df.loc[i, "hour"] = dt.hour
    #     new_df.loc[i, "day_of_week"] = dt.weekday()
    #     if dt.weekday() > 5:
    #         new_df.loc[i, "is_weekend"] = 1
    #     else:
    #         new_df.loc[i, "is_weekend"] = 0
    #
    #     i += 1
    #
    # return new_df

    new_df = df.copy()

    new_df["hour"] = new_df["DateTime"].dt.hour
    new_df["day_of_week"] = new_df["DateTime"].dt.dayofweek
    new_df["is_weekend"] = (new_df["day_of_week"] >= 5).astype(int)

    return new_df


def add_lag_features(
        df: pd.DataFrame,
        segment_column: str,
        target_column: str,
        lags: tuple[int, ...],
) -> pd.DataFrame:
    """
    Add lag features for the target column within each segment.

    Example:
        lag 1 => Vehicles(t-1) within the same Junction

    Args:
        df: Sorted dataframe.
        segment_column: Segment identifier column.
        target_column: Target column to lag (Vehicles).
        lags: Lag steps to create.

    Returns:
        DataFrame with new lag feature columns added.
    """
    new_df = df.copy()
    for lag_num in lags:
        new_df["Vehicles_lag_" + str(lag_num)] = new_df.groupby(segment_column)[target_column].shift(lag_num)
    return new_df


def add_rolling_features(
        df: pd.DataFrame,
        segment_column: str,
        target_column: str,
        windows: tuple[int, ...],
) -> pd.DataFrame:
    """
    Add rolling-window features (within each segment) for the target column.

    Typical rolling stats:
        - rolling mean
        - rolling std

    Args:
        df: Sorted dataframe.
        segment_column: Segment identifier column.
        target_column: Target column to compute rolling stats on.
        windows: Window sizes (in rows).

    Returns:
        DataFrame with new rolling feature columns added.
    """
    new_df = df.copy()
    for window in windows:
        values = []
        for i in range(window):
             values.append(new_df.groupby(segment_column)[target_column].shift(i))
             values.append(new_df.groupby(segment_column)[target_column].shift(i))
        # new_df["Rolling_mean_" + str(window)] =
        # new_df["Rolling_std_" + str(window)] =
    return new_df


def make_supervised_targets(
        df: pd.DataFrame,
        segment_column: str,
        target_column: str,
        horizon: int,
) -> pd.DataFrame:
    """
    Create a supervised learning target for forecasting.

    Example for horizon=1:
        y(t) = Vehicles(t+1) within the same segment

    Adds a new column typically named: f"{target_column}_y"

    Args:
        df: Sorted dataframe.
        segment_column: Segment identifier column.
        target_column: Base signal.
        horizon: Forecast horizon in steps.

    Returns:
        DataFrame with a new target column added.
    """
    raise NotImplementedError


def drop_feature_na_rows(df: pd.DataFrame, target_y_col: str) -> pd.DataFrame:
    """
    Drop rows that have NaNs after creating lag/rolling features and the supervised target.

    Args:
        df: Feature dataframe.
        target_y_col: Name of the supervised target column.

    Returns:
        Clean feature dataframe with no NaNs in required feature/target columns.
    """
    raise NotImplementedError


def build_features(df: pd.DataFrame, config: FeatureConfig) -> pd.DataFrame:
    """
    End-to-end feature build for a cleaned time-series dataframe.

    Steps (typical):
        1) ensure datetime
        2) sort by segment and time
        3) add time features
        4) add lag features
        5) add rolling features
        6) create supervised target y(t+h)
        7) drop rows with NaNs from lag/rolling/target shift

    Args:
        df: Cleaned input dataframe.
        config: FeatureConfig controlling feature generation.

    Returns:
        Model-ready feature dataframe.
    """
    raise NotImplementedError


def save_features(df: pd.DataFrame, path: Path) -> None:
    """
    Save the feature dataframe to disk.

    Args:
        df: Feature dataframe.
        path: Output CSV path (e.g., data/processed/features.csv)
    """
    raise NotImplementedError


def main() -> None:
    """
    CLI entry point for feature building.

    Typical usage:
        python -m src.features.build_features
    """
    raise NotImplementedError


if __name__ == "__main__":

    TRAFFIC_ROOT = Path(__file__).resolve().parents[2] #traffic-ml-system
    TEST_DATA_DIR = TRAFFIC_ROOT / "src" / "test_data"

    df_features_horizon = load_processed_timeseries(
        TEST_DATA_DIR / "test_features_horizon.csv"
    )

    df_features_multi_segment = load_processed_timeseries(
        TEST_DATA_DIR / "test_features_multi_segment.csv"
    )

    df_features_short_series = load_processed_timeseries(
        TEST_DATA_DIR / "test_features_short_series.csv"
    )

    df_features_unsorted = load_processed_timeseries(
        TEST_DATA_DIR / "test_features_unsorted.csv"
    )

    df_features_valid_small = load_processed_timeseries(
        TEST_DATA_DIR / "test_features_valid_small.csv"
    )

    df_features_weekend_span = load_processed_timeseries(
        TEST_DATA_DIR / "test_features_weekend_span.csv"
    )

    df_lag_multi_segment = load_processed_timeseries(
        TEST_DATA_DIR / "test_lag_multi_segment.csv"
    )

    #testing add_time_features
    df = ensure_datetime(df_features_horizon, "DateTime")
    df = add_time_features(df, "DateTime")
    i = 0
    for hour in df["hour"]:
        print(str(hour) + ", " +str(df["day_of_week"][i]) + ", " + str(df["is_weekend"][i]))
        i+=1

    # testing add_lag_features
    print(df)
    df = add_lag_features(df_lag_multi_segment, "Junction", "Vehicles", (1,))
    print(df)


    #main()
