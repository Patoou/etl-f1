from __future__ import annotations
import pandas as pd

ALLOWED = {"S","M","H","I","W"}

def validate_strategy(df: pd.DataFrame) -> None:
    assert {"strategy_key","stint_laps","pit_count","compounds"}.issubset(df.columns)

    # dominios y tamaños
    assert df["pit_count"].ge(0).all()
    for _, r in df.iterrows():
        stints = r["stint_laps"]
        comps  = [c for c in r["strategy_key"].split("-") if c]
        assert all(isinstance(x, int) and x >= 1 for x in stints)
        if len(comps) > 0:
            assert len(comps) == len(stints)
            assert all(c in ALLOWED for c in comps)
        # coherencia pit_count
        assert r["pit_count"] == max(len(stints) - 1, 0)
        # total_laps
        if pd.notna(r.get("total_laps", None)):
            assert sum(stints) <= int(r["total_laps"])
