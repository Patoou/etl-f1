from __future__ import annotations
from pathlib import Path
import json
import pandas as pd
import numpy as np
from typing import List, Iterable

ALLOWED = {"S","M","H","I","W"}
ORDER = ["S","M","H","I","W"]  # orden “seco→lluvia”

def _kendall_tau(seq: List[str], order=ORDER):
    seq = [c for c in seq if c in order]
    n = len(seq)
    if n < 2: return 0.0
    vals = [order.index(c) for c in seq]
    conc = disc = 0
    for i in range(n-1):
        for j in range(i+1, n):
            if vals[i] == vals[j]:
                continue
            if vals[i] < vals[j]: conc += 1
            else: disc += 1
    denom = conc + disc
    return 0.0 if denom == 0 else (conc - disc) / denom

def _read_flags(sdir: Path) -> int:
    p = sdir / "flags.json"
    if not p.exists(): return 0
    try:
        return int(json.loads(p.read_text()).get("rain_flag", 0))
    except Exception:
        return 0

def resolve_silver_dir(silver_base: str, rid: str) -> Path:
    p = Path(silver_base) / rid
    if not p.exists():
        raise FileNotFoundError(f"Silver no encontrado: {p}")
    return p

# ---- helpers robustos para columnas tipo lista ----
def _as_list(x) -> list:
    if isinstance(x, list): return x
    if isinstance(x, tuple): return list(x)
    if isinstance(x, np.ndarray): return x.tolist()
    if x is None or (isinstance(x, float) and pd.isna(x)): return []
    # Evitar strings iterables
    if isinstance(x, (str, bytes)): return [x]
    try:
        return list(x)  # último recurso (e.g., Arrow ListValue)
    except Exception:
        return [x] if x is not None else []

def _avg_len(xs: Iterable) -> float | None:
    xs = _as_list(xs)
    xs = [int(v) for v in xs if pd.notna(v)]
    return (float(sum(xs)) / len(xs)) if xs else None

def _uniq_count(xs: Iterable) -> int:
    xs = _as_list(xs)
    return len({c for c in xs if isinstance(c, str) and c})

def build_driver_strategy(sdir: Path) -> pd.DataFrame:
    laps   = pd.read_parquet(sdir/"laps_silver.parquet", engine="pyarrow")
    res    = pd.read_parquet(sdir/"results_silver.parquet", engine="pyarrow")
    stints = pd.read_parquet(sdir/"driver_stints.parquet", engine="pyarrow")

    # metadatos
    rid = sdir.name
    season = int(rid.split("_", 1)[0])
    rain_flag = _read_flags(sdir)
    total_laps = int(laps["lap_number"].max()) if len(laps) else None

    # normalizar keys
    if "driver_number" in stints.columns:
        stints["driver_number"] = pd.to_numeric(stints["driver_number"], errors="coerce").astype("Int64")
    if "driver_number" in res.columns:
        res["driver_number"] = pd.to_numeric(res["driver_number"], errors="coerce").astype("Int64")

    key = ["driver_number"]
    if ("driver_number" not in stints.columns or
        "driver_number" not in res.columns or
        stints["driver_number"].isna().all()):
        key = ["driver"]

    # evitar overlap de columnas
    res_keep_pref = [c for c in ["grid","final_pos","finish_status"] if c in res.columns]
    res_use = res[key + res_keep_pref].drop_duplicates(subset=key)

    base = stints.merge(res_use, how="left", on=key)

    # tipos y defaults
    for col in ("driver_number","grid","final_pos"):
        if col not in base.columns:
            base[col] = pd.NA
        base[col] = pd.to_numeric(base[col], errors="coerce").astype("Int64")

    # coerción segura de columnas lista
    base["stint_laps"] = base["stint_laps"].apply(_as_list)
    base["compounds"]  = base["compounds"].apply(_as_list)

    # features derivados
    base["season"] = season
    base["race_id"] = rid
    base["rain_flag"] = rain_flag
    base["total_laps"] = total_laps
    base["first_compound"] = base["strategy_key"].str.split("-").str[0]
    base["last_compound"]  = base["strategy_key"].str.split("-").str[-1]
    base["unique_comp_count"] = base["compounds"].apply(_uniq_count)
    base["avg_stint_len"] = base["stint_laps"].apply(_avg_len)
    base["uses_wet"] = base["strategy_key"].str.contains(r"(I|W)", na=False).astype(int)
    base["order_kendall_tau"] = base["strategy_key"].apply(
        lambda s: _kendall_tau(s.split("-")) if isinstance(s, str) and s else 0.0
    )

    keep = [
        "season","race_id","driver","driver_number","team",
        "grid","final_pos","rain_flag","pit_count","stint_count",
        "strategy_key","stint_laps","compounds","first_compound","last_compound",
        "unique_comp_count","avg_stint_len","uses_wet","order_kendall_tau","total_laps"
    ]
    return base[keep].reset_index(drop=True)
