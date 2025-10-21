import os, pytest
from pathlib import Path
from src.etl.bronze.run import run as run_bronze
from src.etl.silver.run import run as run_silver
from src.etl.gold.run import run as run_gold

@pytest.mark.skipif(os.getenv("RUN_FASTF1_E2E") != "1", reason="set RUN_FASTF1_E2E=1 to run")
def test_gold_end_to_end(tmp_path):
    cache = "data/.fastf1_cache"
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    gold   = tmp_path / "gold"

    run_bronze(2022, "Silverstone", "R", cache=cache, out=str(bronze))
    run_silver(2022, "Silverstone", "R", cache=cache, bronze=str(bronze), out=str(silver))
    run_gold(silver=str(silver), out=str(gold), season=2022, event="Silverstone")

    gdir = next(gold.glob("2022_Silverstone"))
    assert (gdir/"driver_strategy.parquet").exists()
    assert (gdir/"schema.json").exists()
    assert (gdir/"train.parquet").exists()
    assert (gdir/"test.parquet").exists()
