from __future__ import annotations
import pandas as pd
from typing import List

ALLOWED = {"S","M","H","I","W"}

def _ordered_unique(seq: List[str]) -> List[str]:
    seen = set(); out = []
    for x in seq:
        if x is None: continue
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def build_stints(laps_silver: pd.DataFrame) -> pd.DataFrame:
    df = laps_silver.sort_values(["driver","lap_number"]).copy()

    def _per_driver(g: pd.DataFrame) -> pd.DataFrame:
        g = g.copy().sort_values("lap_number")
        by_comp = g["compound"] != g["compound"].shift(1)
        by_stint = g["stint_raw"] != g["stint_raw"].shift(1)
        pit_evt = g["pit_in_ms"].notna() | g["pit_out_ms"].notna()
        start = pd.Series(True, index=g.index)
        new_stint = start | by_comp | by_stint | pit_evt
        g["stint_id"] = new_stint.cumsum()

        agg = g.groupby("stint_id", as_index=False).agg(
            stint_laps=("lap_number", "count"),
            compound=("compound", lambda s: s.dropna().iloc[0] if len(s.dropna()) else None),
        )

        compounds = [c if c in ALLOWED else None for c in agg["compound"].tolist()]
        strategy_key = "-".join([c for c in compounds if c])
        stint_laps = agg["stint_laps"].tolist()
        pit_count = max(len(agg) - 1, 0)

        return pd.DataFrame({
            "driver": [g["driver"].iloc[0]],
            "driver_number": [g["driver_number"].iloc[0]],
            "team": [g["team"].mode().iloc[0] if g["team"].notna().any() else None],
            "stint_count": [len(agg)],
            "pit_count": [pit_count],
            "strategy_key": [strategy_key],
            "stint_laps": [stint_laps],
            "compounds": [_ordered_unique(compounds)],
            "max_lap_completed": [int(g["lap_number"].max()) if g["lap_number"].notna().any() else None],
        })

    per_driver_list = []
    for _, g in df.groupby("driver", sort=True):
        per_driver_list.append(_per_driver(g))
    per_driver = pd.concat(per_driver_list, ignore_index=True)
    return per_driver
