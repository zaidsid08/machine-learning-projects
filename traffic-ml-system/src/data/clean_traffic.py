"""
Traffic data cleaning pipeline.

This module is responsible for loading raw traffic CSV data,
validating and parsing timestamps, cleaning invalid rows,
and producing a processed time-series dataset suitable for
feature engineering and machine learning.
"""

from pathlib import Path
import pandas as pd



def load_raw_traffic_data(file_path: Path) -> pd.DataFrame:
    """
    Load raw traffic data from a CSV file.

    Args:
        file_path (Path): Path to the raw traffic CSV file.

    Returns:
        pd.DataFrame: Raw traffic data as loaded from disk.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Raw traffic data file not found: {file_path}")

    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        raise ValueError(f"Failed to read CSV file: {e}")

    if df.empty:
        raise ValueError("Raw traffic CSV is empty.")

    return df


def parse_timestamps(
        df: pd.DataFrame,
        time_column: str
) -> pd.DataFrame:
    """
    Parse and validate timestamp column.

    Converts timestamp strings into pandas datetime objects
    and removes rows with invalid or missing timestamps.

    Args:
        df (pd.DataFrame): Input dataframe.
        time_column (str): Name of the timestamp column.

    Returns:
        pd.DataFrame: Dataframe with parsed and validated timestamps.
    """
    #go through every timestamp
    row = 0
    for time in df[time_column]:
        # remove from df if not a valid timestamp
        if pd.isna(pd.to_datetime(time, errors="coerce")):
            #index number on csv is increased by 2 to be the row number
            # since header removed and second line is index 0
            print("removed row number " + str(row + 2) + " of csv: " + str(df[time_column][row]))
            df = df.drop(index=row)

        row += 1
    # once all invalid timestamp rows have been removed,
    # convert the entire column of valid timestamp strings into
    # pandas datetime objects
    df[time_column] = pd.to_datetime(df[time_column])
    return df



def select_relevant_columns(
        df: pd.DataFrame,
        segment_column: str,
        time_column: str,
        target_column: str
) -> pd.DataFrame:
    """
    Select only the columns required for modeling.

    Args:
        df (pd.DataFrame): Input dataframe.
        segment_column (str): Column identifying traffic location.
        time_column (str): Timestamp column.
        target_column (str): Traffic signal (e.g., volume or speed).

    Returns:
        pd.DataFrame: Reduced dataframe with selected columns.
    """
    for col in df:
        if not (col == segment_column or col == time_column or col == target_column):
            df = df.drop(columns=[col])
    return df


def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows containing missing or invalid values.

    Args:
        df (pd.DataFrame): Input dataframe.

    Returns:
        pd.DataFrame: Cleaned dataframe with invalid rows removed.
    """
    pass


def sort_time_series(
        df: pd.DataFrame,
        segment_column: str,
        time_column: str
) -> pd.DataFrame:
    """
    Sort data by segment and timestamp to prepare for time-series operations.

    Args:
        df (pd.DataFrame): Input dataframe.
        segment_column (str): Traffic segment identifier.
        time_column (str): Timestamp column.

    Returns:
        pd.DataFrame: Time-ordered dataframe.
    """
    pass


def save_processed_data(
        df: pd.DataFrame,
        output_path: Path
) -> None:
    """
    Save processed traffic data to disk.

    Args:
        df (pd.DataFrame): Cleaned dataframe.
        output_path (Path): Destination path for processed data.

    Returns:
        None
    """
    pass


def run_cleaning_pipeline(
        raw_data_path: Path,
        output_path: Path
) -> None:
    """
    Run the full traffic data cleaning pipeline.

    Orchestrates loading raw data, parsing timestamps,
    cleaning invalid rows, sorting time-series data,
    and saving the processed result.

    Args:
        raw_data_path (Path): Path to raw traffic CSV.
        output_path (Path): Path to save processed data.

    Returns:
        None
    """
    pass


if __name__ == "__main__":
    """
    Entry point for running the cleaning pipeline as a script.
    """
    DATA_ROOT = Path(__file__).resolve().parents[0]
    # RAW_DATA_PATH = DATA_ROOT / "raw" / "traffic.csv"
    # df = load_raw_traffic_data(RAW_DATA_PATH)
    # df = parse_timestamps(df, "DateTime")
    # # testing if all invalid time stamps have been removed form datetime column
    # for time in df["DateTime"]:
    #     if not isinstance(time, pd.Timestamp):
    #         print(False)
    #         break
    # testing select_relevant_columns:
    RAW_DATA_PATH_WITH_EXTRA_COLS = DATA_ROOT / "raw" / ("traffic_extra_cols.csv")
    extra_cols_df = load_raw_traffic_data(RAW_DATA_PATH_WITH_EXTRA_COLS)
    select_relevant_columns(extra_cols_df, "Junction", "Datetime", "Vehicles")
    for col in extra_cols_df:
        print(col)


    # RAW_DATA_PATH = Path("data/raw/traffic.csv")
    # OUTPUT_PATH = Path("data/processed/segment_timeseries.csv")
    #
    # run_cleaning_pipeline(RAW_DATA_PATH, OUTPUT_PATH)
