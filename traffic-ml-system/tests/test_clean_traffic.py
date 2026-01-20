"""
Pytest test suite for src/data/clean_traffic.py

How to use:
1) Put your testing CSVs in: traffic-ml-system/test_data/
   (same level as src/ and tests/)

2) Install pytest:
   pip install pytest

3) Run from traffic-ml-system/:
   pytest -q
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd
import pytest

# Import the functions you implemented in clean_traffic.py
from data.clean_traffic import (
    load_raw_traffic_data,
    parse_timestamps,
    select_relevant_columns,
    remove_invalid_rows,
    sort_time_series,
    save_processed_data,
    run_cleaning_pipeline,
)

# ---------------------------
# Test data helpers
# ---------------------------

TEST_DATA_DIR = Path(__file__).resolve().parents[1] / "test_data"


def require_file(filename: str) -> Path:
    """Return path to a test CSV; skip test if the file isn't present."""
    path = TEST_DATA_DIR / filename
    if not path.exists():
        pytest.skip(f"Missing test file: {path}. Put it in {TEST_DATA_DIR}")
    return path


# ---------------------------
# load_raw_traffic_data
# ---------------------------

def test_load_raw_traffic_data_reads_csv() -> None:
    path = require_file("test_missing_hours.csv")
    df = load_raw_traffic_data(path)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_load_raw_traffic_data_missing_file_raises() -> None:
    missing = TEST_DATA_DIR / "does_not_exist.csv"
    with pytest.raises(FileNotFoundError):
        load_raw_traffic_data(missing)


# ---------------------------
# parse_timestamps
# ---------------------------

def test_parse_timestamps_converts_to_datetime_dtype() -> None:
    path = require_file("test_missing_hours.csv")
    df = load_raw_traffic_data(path)

    out = parse_timestamps(df, time_column="DateTime")

    assert "DateTime" in out.columns
    assert pd.api.types.is_datetime64_any_dtype(out["DateTime"])


def test_parse_timestamps_keeps_valid_rows_and_drops_invalid() -> None:
    # This test expects a file that contains invalid timestamps.
    # If you don’t have one, add it to test_data/ (e.g., from earlier: traffic_invalid_timestamps.csv)
    # or rename/copy it to: test_invalid_timestamps.csv
    path = None
    # Prefer the "test_" naming if you have it
    for candidate in ["test_invalid_timestamps.csv", "traffic_invalid_timestamps.csv"]:
        try:
            path = require_file(candidate)
            break
        except pytest.skip.Exception:
            pass

    if path is None:
        pytest.skip("No invalid timestamp dataset found in test_data/")

    df = load_raw_traffic_data(path)
    out = parse_timestamps(df, time_column="DateTime")

    # should drop at least one invalid row
    assert len(out) < len(df)

    # after parsing, no NaT should remain
    assert out["DateTime"].isna().sum() == 0


# ---------------------------
# select_relevant_columns
# ---------------------------

def test_select_relevant_columns_keeps_only_expected_cols() -> None:
    path = require_file("test_extra_irrelevant_columns.csv")
    df = load_raw_traffic_data(path)

    out = select_relevant_columns(
        df,
        segment_column="Junction",
        time_column="DateTime",
        target_column="Vehicles",
    )

    assert list(out.columns) == ["Junction", "DateTime", "Vehicles"] or list(out.columns) == ["DateTime", "Junction", "Vehicles"]
    assert set(out.columns) == {"Junction", "DateTime", "Vehicles"}


def test_select_relevant_columns_missing_required_col_raises() -> None:
    path = require_file("test_schema_different_column_names.csv")
    df = load_raw_traffic_data(path)

    # This dataset uses: timestamp, segment_id, volume, row_id
    # So selecting Kaggle-style columns should fail cleanly.
    with pytest.raises((KeyError, ValueError)):
        select_relevant_columns(
            df,
            segment_column="Junction",
            time_column="DateTime",
            target_column="Vehicles",
        )


# If you later add a function that renames alternative schemas -> standard schema,
# you can turn this into a "success" test.
@pytest.mark.xfail(reason="Enable when you implement schema renaming/standardization.")
def test_select_relevant_columns_supports_alternate_schema() -> None:
    path = require_file("test_schema_different_column_names.csv")
    df = load_raw_traffic_data(path)

    out = select_relevant_columns(
        df,
        segment_column="segment_id",
        time_column="timestamp",
        target_column="volume",
    )
    assert set(out.columns) == {"segment_id", "timestamp", "volume"}


# ---------------------------
# remove_invalid_rows
# ---------------------------

def test_remove_invalid_rows_drops_bad_id_rows() -> None:
    path = require_file("test_id_missing_and_duplicate.csv")
    df = load_raw_traffic_data(path)

    # Some pipelines enforce:
    # - no missing ID
    # - ID uniqueness
    # If your implementation only drops missing values but doesn't enforce uniqueness yet,
    # that's okay — this test checks at least missing IDs are handled.
    out = remove_invalid_rows(df)

    # at minimum, no missing IDs if you treat ID as required
    if "ID" in out.columns:
        assert out["ID"].isna().sum() == 0


def test_remove_invalid_rows_handles_numeric_whitespace_if_supported() -> None:
    path = require_file("test_convertible_numeric_with_whitespace.csv")
    df = load_raw_traffic_data(path)

    # If you haven't implemented stripping/coercion yet, this might raise.
    # That's fine early on; once implemented, this should pass.
    try:
        out = remove_invalid_rows(df)
        assert not out.empty
    except (ValueError, TypeError):
        pytest.xfail("remove_invalid_rows does not yet coerce/strip whitespace numeric fields.")


# Optional: if you have these older datasets in test_data/
@pytest.mark.xfail(reason="Enable when your remove_invalid_rows enforces strict Junction validity (int >= 1).")
def test_remove_invalid_rows_drops_invalid_junctions() -> None:
    path = None
    for candidate in ["traffic_invalid_junctions.csv", "test_invalid_junctions.csv"]:
        try:
            path = require_file(candidate)
            break
        except pytest.skip.Exception:
            pass
    if path is None:
        pytest.skip("No invalid junction dataset found in test_data/")

    df = load_raw_traffic_data(path)
    out = remove_invalid_rows(df)

    # expect only valid positive integers remain
    assert out["Junction"].isna().sum() == 0
    assert (out["Junction"] >= 1).all()
    assert pd.api.types.is_integer_dtype(out["Junction"])


@pytest.mark.xfail(reason="Enable when your remove_invalid_rows enforces strict Vehicles validity (int >= 0).")
def test_remove_invalid_rows_drops_invalid_vehicles() -> None:
    path = None
    for candidate in ["traffic_invalid_vehicles.csv", "test_invalid_vehicles.csv"]:
        try:
            path = require_file(candidate)
            break
        except pytest.skip.Exception:
            pass
    if path is None:
        pytest.skip("No invalid vehicles dataset found in test_data/")

    df = load_raw_traffic_data(path)
    out = remove_invalid_rows(df)

    assert out["Vehicles"].isna().sum() == 0
    assert (out["Vehicles"] >= 0).all()
    assert pd.api.types.is_integer_dtype(out["Vehicles"])


# ---------------------------
# sort_time_series
# ---------------------------

def test_sort_time_series_sorts_by_segment_then_time() -> None:
    path = require_file("test_weekly_realistic_valid.csv")
    df = load_raw_traffic_data(path)

    # ensure DateTime is parsed before sort (depends on your design)
    df = parse_timestamps(df, time_column="DateTime")

    out = sort_time_series(df, segment_column="Junction", time_column="DateTime")

    # Check monotonicity per junction group
    for j, grp in out.groupby("Junction"):
        assert grp["DateTime"].is_monotonic_increasing


def test_sort_time_series_fixes_unsorted_data() -> None:
    path = require_file("traffic_unsorted_timeseries.csv")
    df = load_raw_traffic_data(path)
    df = parse_timestamps(df, time_column="DateTime")

    out = sort_time_series(df, segment_column="Junction", time_column="DateTime")

    for j, grp in out.groupby("Junction"):
        assert grp["DateTime"].is_monotonic_increasing


# ---------------------------
# save_processed_data
# ---------------------------

def test_save_processed_data_writes_csv(tmp_path: Path) -> None:
    path = require_file("test_year_boundary_valid.csv")
    df = load_raw_traffic_data(path)

    out_path = tmp_path / "out.csv"
    save_processed_data(df, out_path)

    assert out_path.exists()
    assert out_path.stat().st_size > 0


# ---------------------------
# run_cleaning_pipeline (end-to-end)
# ---------------------------

def test_run_cleaning_pipeline_end_to_end_creates_output(tmp_path: Path) -> None:
    """
    End-to-end test:
    - loads raw
    - selects columns
    - parses timestamps
    - removes invalid rows
    - sorts
    - saves output
    """
    raw_path = require_file("test_extra_irrelevant_columns.csv")
    out_path = tmp_path / "segment_timeseries.csv"

    run_cleaning_pipeline(raw_path, out_path)

    assert out_path.exists()
    df_out = pd.read_csv(out_path)
    assert not df_out.empty

    # Output should contain at least the core columns
    # (Depending on your exact output ordering)
    assert {"DateTime", "Junction", "Vehicles"}.issubset(set(df_out.columns))


# ---------------------------
# Optional: duplicate timestamp behavior
# ---------------------------

@pytest.mark.xfail(reason="Enable if/when you implement a policy for duplicate timestamps per Junction.")
def test_duplicate_timestamps_policy() -> None:
    """
    This dataset has duplicate DateTime within the same Junction.
    Decide your policy later (drop duplicates, average, keep first, etc.),
    then update this test to match the policy.
    """
    path = require_file("test_duplicate_timestamps_same_junction.csv")
    df = load_raw_traffic_data(path)
    df = parse_timestamps(df, time_column="DateTime")

    out = sort_time_series(df, segment_column="Junction", time_column="DateTime")

    # Example policy: no duplicates remain:
    dup_count = out.duplicated(subset=["Junction", "DateTime"]).sum()
    assert dup_count == 0
