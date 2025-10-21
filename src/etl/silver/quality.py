from __future__ import annotations
import pandas as pd

ALLOWED = {"S", "M", "H", "I", "W"}

def validate_no_lap_dupes(laps_silver: pd.DataFrame) -> None:
    """Valida unicidad por (driver, lap_number)."""
    dupes = laps_silver.duplicated(subset=["driver", "lap_number"], keep=False)
    assert not dupes.any(), f"Duplicados en laps: {int(dupes.sum())} filas"

def validate_stints(driver_stints: pd.DataFrame, laps_silver: pd.DataFrame) -> None:
    assert driver_stints["pit_count"].ge(0).all()
    for _, r in driver_stints.iterrows():
        n_stints = len(r["stint_laps"])
        # pit_count ↔ stints
        assert r["pit_count"] == max(n_stints - 1, 0)
        # largo de strategy_key
        assert len(r["strategy_key"].split("-")) in {0, n_stints}
        # dominio compuestos
        for c in r["compounds"]:
            assert c in ALLOWED
        # suma de stints ≤ laps reales del piloto
        drv = r["driver"]
        real_max = laps_silver.loc[laps_silver["driver"] == drv, "lap_number"].max()
        assert sum(r["stint_laps"]) <= int(real_max)
