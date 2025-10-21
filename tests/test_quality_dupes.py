import pandas as pd
import pytest
from src.etl.silver.quality import validate_no_lap_dupes

def test_no_lap_dupes_detects_duplicates():
    df = pd.DataFrame({
        "driver": ["VER", "VER", "HAM"],
        "lap_number": [10, 10, 1],
    })
    with pytest.raises(AssertionError):
        validate_no_lap_dupes(df)

def test_no_lap_dupes_ok():
    df = pd.DataFrame({
        "driver": ["VER", "VER", "HAM"],
        "lap_number": [10, 11, 1],
    })
    validate_no_lap_dupes(df)  # no error
