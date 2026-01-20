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
def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows containing missing or invalid values.

    Args:
        df (pd.DataFrame): Input dataframe.

    Returns:
        pd.DataFrame: Cleaned dataframe with invalid rows removed.
    """
    i = 0
    IDs = []
    for col in df:
        for row in df[col]:
            if col == "DateTime":
                if pd.isna(pd.to_datetime(row, errors="coerce")):
                    print("removed row number " + str(row + 2) + " of csv: " + str(df[col][row]))
                    df = df.drop(index=i)
                elif col == "Junction" or col == "Vehicles":
                    if not is_positive_integer(str(row)):
                        df = df.drop(index=i)
                elif col == "ID" and is_positive_integer(str(row)):
                    if row not in IDs:
                        IDs.append(row)
                    else:
                        df = df.drop(index=i)
                i += 1


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

# RAW_DATA_PATH = Path("data/raw/traffic.csv")
    # OUTPUT_PATH = Path("data/processed/segment_timeseries.csv")
    #
    # run_cleaning_pipeline(RAW_DATA_PATH, OUTPUT_PATH)
