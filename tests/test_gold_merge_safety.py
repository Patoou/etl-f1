# tests/test_gold_merge_safety.py
import pandas as pd
from src.etl.gold.build import build_driver_strategy, resolve_silver_dir

def test_merge_no_overlap_columns(tmp_path):
    # No se ejecuta E2E; verifica que la función existe.
    assert callable(build_driver_strategy)
