import os
import pytest
from pathlib import Path
from src.etl.silver.run import run as run_silver
from src.etl.bronze.run import run as run_bronze

# Ejecuta end-to-end si seteás RUN_FASTF1_E2E=1
@pytest.mark.skipif(os.getenv("RUN_FASTF1_E2E") != "1", reason="set RUN_FASTF1_E2E=1 to run")
def test_silver_pipeline_tmp(tmp_path):
    cache = "data/.fastf1_cache"
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"

    # Bronze (por si no existe)
    run_bronze(2022, "Silverstone", "R", cache=cache, out=str(bronze))
    # Silver
    run_silver(2022, "Silverstone", "R", cache=cache, bronze=str(bronze), out=str(silver))

    sdir = next(silver.glob("2022_Silverstone"))
    assert (sdir/"laps_silver.parquet").exists()
    assert (sdir/"driver_stints.parquet").exists()

    import pandas as pd
    st = pd.read_parquet(sdir/"driver_stints.parquet")
    assert {"driver","pit_count","strategy_key","stint_laps","compounds"} <= set(st.columns)
    # coherencia básica
    row = st.iloc[0]
    assert row["pit_count"] == max(len(row["stint_laps"]) - 1, 0)
