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
        # remove column if name does not match with one of the names from the
        # parameter column names
        if not (col == segment_column or col == time_column or col == target_column):
            df = df.drop(columns=[col])
    return df

def is_positive_integer(s: str) -> bool:
    """
    Determine whether a string can be safely converted to a positive integer.

    This function strips leading and trailing whitespace and attempts to
    convert the string to an integer. The value is considered valid only
    if the conversion succeeds and the integer is greater than zero.

    Args:
        s (str): Input string to check.

    Returns:
        bool: True if the string is a valid positive integer, False otherwise.
    """
    try:
        num = int(s)
    except ValueError:
        return False
    if num > 0:
        return True
    return False



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
def remove_invalid_rows(
        df: pd.DataFrame,
        segment_column: str = "Junction",
        time_column: str = "DateTime",
        target_column: str = "Vehicles",
        id_column: str = "ID",
) -> pd.DataFrame:
    """
    Remove rows containing missing or invalid values.

    Rules (row is kept only if ALL are true):
      - time_column parses to a valid datetime
      - segment_column is a positive integer
      - target_column is a positive integer
      - id_column is a positive integer AND unique within the dataframe

    Args:
        df (pd.DataFrame): Input dataframe.
        segment_column (str): Column identifying traffic location.
        time_column (str): Timestamp column.
        target_column (str): Traffic signal column (e.g., volume).
        id_column (str): Unique row identifier column.

    Returns:
        pd.DataFrame: Cleaned dataframe with invalid rows removed.
    """
    bad_idxs: list[int] = []
    seen_ids: set[int] = set()

    # iterate row-by-row (safe), collect bad indices, drop once at end
    for idx, r in df.iterrows():
        # --- timestamp valid ---
        dt = pd.to_datetime(r.get(time_column), errors="coerce")
        if pd.isna(dt):
            bad_idxs.append(idx)
            continue

        # --- junction + vehicles valid positive ints ---
        junction_val = r.get(segment_column)
        vehicles_val = r.get(target_column)

        if not is_positive_integer(str(junction_val)) or not is_positive_integer(str(vehicles_val)):
            bad_idxs.append(idx)
            continue

        # --- id valid + unique ---
        id_val = r.get(id_column)
        if not is_positive_integer(str(id_val)):
            bad_idxs.append(idx)
            continue

        id_int = int(str(id_val).strip())
        if id_int in seen_ids:
            bad_idxs.append(idx)
            continue
        seen_ids.add(id_int)

    return df.drop(index=bad_idxs)


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
    TRAFFIC_ROOT = Path(__file__).resolve().parents[2]   # traffic-ml-system/
    TEST_DATA_DIR = TRAFFIC_ROOT / "src" / "test_data"

    df_convertible_numeric = load_raw_traffic_data(
        TEST_DATA_DIR / "test_convertible_numeric_with_whitespace.csv"
    )

    df_duplicate_timestamps = load_raw_traffic_data(
        TEST_DATA_DIR / "test_duplicate_timestamps_same_junction.csv"
    )

    df_extra_columns = load_raw_traffic_data(
        TEST_DATA_DIR / "test_extra_irrelevant_columns.csv"
    )

    df_id_missing_and_duplicate = load_raw_traffic_data(
        TEST_DATA_DIR / "test_id_missing_and_duplicate.csv"
    )

    df_missing_hours = load_raw_traffic_data(
        TEST_DATA_DIR / "test_missing_hours.csv"
    )

    df_schema_diff_names = load_raw_traffic_data(
        TEST_DATA_DIR / "test_schema_different_column_names.csv"
    )

    df_weekly_realistic = load_raw_traffic_data(
        TEST_DATA_DIR / "test_weekly_realistic_valid.csv"
    )

    df_year_boundary = load_raw_traffic_data(
        TEST_DATA_DIR / "test_year_boundary_valid.csv"
    )

    all_dfs = [df_convertible_numeric, df_duplicate_timestamps,
               df_extra_columns, df_id_missing_and_duplicate, df_missing_hours,
               df_schema_diff_names, df_weekly_realistic, df_year_boundary]
    # testing all methods on all dataframes of all test csvs
    # 1. testing parse_timestamps on all dfs
    for df in all_dfs:
        if df is df_schema_diff_names:
            parse_timestamps(df, "timestamp")
            for row in df["timestamp"]:
                if not isinstance(row, pd.Timestamp):
                    print(False)
        else:
            parse_timestamps(df, "DateTime")
            for row in df["DateTime"]:
                    if not isinstance(row, pd.Timestamp):
                        print(False)

    # 2. testing select_relevant_timestamps
    i = 0
    for df in all_dfs:

        if df is df_schema_diff_names:
            all_dfs[i] = select_relevant_columns(df, "segment_id", "timestamp", "volume")
        else:
            all_dfs[i] = select_relevant_columns(df, "Junction", "DateTime", "Vehicles")
        i += 1


    for df in all_dfs:
        for col in df:
                if col not in ["segment_id", "timestamp", "volume", "Junction", "DateTime", "Vehicles"]:
                    print(str(col) + " is not supposed to be in df but is still here")

    # 3. testing remove_invalid_rows
    all_dfs = [df_convertible_numeric, df_duplicate_timestamps,
               df_extra_columns, df_id_missing_and_duplicate, df_missing_hours,
               df_schema_diff_names, df_weekly_realistic, df_year_boundary]
    i = 0
    for df in all_dfs:
        if df is df_schema_diff_names:
            segment_column, time_column, target_column, id_column = "segment_id", "timestamp", "volume", "row_id"
            all_dfs[i] = remove_invalid_rows(df, "segment_id", "timestamp", "volume", "row_id")
        else:
            segment_column, time_column, target_column, id_column = "Junction", "DateTime", "Vehicles", "ID"
            all_dfs[i] = remove_invalid_rows(df, "Junction", "DateTime", "Vehicles", "ID")
        for row in df[time_column]:


        i += 1


    # RAW_DATA_PATH = Path("data/raw/traffic.csv")
    # OUTPUT_PATH = Path("data/processed/segment_timeseries.csv")
    #
    # run_cleaning_pipeline(RAW_DATA_PATH, OUTPUT_PATH)
