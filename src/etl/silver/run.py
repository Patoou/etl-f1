from __future__ import annotations
from pathlib import Path
import pandas as pd
from ..bronze.utils import enable_cache, get_session, race_id as raceid, _slugify, _event_year
from ..bronze.session_io import write_parquet
from .normalize import (
    normalize_laps,
    normalize_weather,
    normalize_results,
    compute_rain_flag,
    RAIN_MM_THRESHOLD,
)
from .stints import build_stints
from .quality import validate_stints, validate_no_lap_dupes

def silver_dir(base="data/silver", rid: str | None = None) -> Path:
    p = Path(base) / (rid or "")
    p.mkdir(parents=True, exist_ok=True)
    return p

def _unique(seq):
    seen = set(); out = []
    for x in seq:
        if x and x not in seen:
            seen.add(x); out.append(x)
    return out

def resolve_bronze_dir(bronze_base: str, ses, rid_override: str | None = None) -> Path:
    base = Path(bronze_base)
    if rid_override:
        p = base / rid_override
        if p.exists():
            return p
        raise FileNotFoundError(f"Bronze no encontrado (override): {p}")

    rid_now = raceid(ses)
    p_now = base / rid_now
    if p_now.exists():
        return p_now

    year = _event_year(ses)
    names = [
        getattr(getattr(ses, "event", {}), "get", lambda *_: None)("Location"),
        getattr(getattr(ses, "event", {}), "get", lambda *_: None)("EventName"),
        getattr(getattr(ses, "event", {}), "get", lambda *_: None)("OfficialEventName"),
        getattr(getattr(ses, "event", {}), "get", lambda *_: None)("Country"),
    ]
    candidates = _unique([f"{year}_{_slugify(str(n))}" for n in names if n])
    for rid in candidates:
        p = base / rid
        if p.exists():
            return p

    options = [d.name for d in base.glob(f"{year}_*") if d.is_dir()]
    raise FileNotFoundError(
        f"Bronze no encontrado. Probé: {[rid_now, *candidates]}. Opciones: {options or 'ninguna'}"
    )

def run(season: int, event: str, kind: str = "R",
        cache: str = "data/.fastf1_cache", bronze: str = "data/bronze", out: str = "data/silver",
        rid: str | None = None, rain_mm_threshold: float = RAIN_MM_THRESHOLD):
    enable_cache(cache)
    ses = get_session(season, event, kind)
    bdir = resolve_bronze_dir(bronze, ses, rid_override=rid)
    print(f"[Silver] Usando Bronze → {bdir}")

    laps_b    = pd.read_parquet(bdir / "laps_raw.parquet",    engine="pyarrow")
    weather_b = pd.read_parquet(bdir / "weather_raw.parquet", engine="pyarrow")
    results_b = pd.read_parquet(bdir / "results_raw.parquet", engine="pyarrow")

    laps_s = normalize_laps(laps_b)
    validate_no_lap_dupes(laps_s)

    weather_s = normalize_weather(weather_b)
    results_s = normalize_results(results_b)
    rain_flag = compute_rain_flag(weather_s, laps_s, mm_threshold=rain_mm_threshold)

    driver_stints = build_stints(laps_s)
    validate_stints(driver_stints, laps_s)

    sdir = silver_dir(out, bdir.name)
    write_parquet(laps_s, sdir / "laps_silver.parquet")
    write_parquet(weather_s, sdir / "weather_silver.parquet")
    write_parquet(results_s, sdir / "results_silver.parquet")
    write_parquet(driver_stints, sdir / "driver_stints.parquet")
    (sdir / "flags.json").write_text(f'{{"rain_flag": {rain_flag}}}')
    print(f"SILVER listo → {sdir}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--event", type=str, required=True)
    ap.add_argument("--kind", type=str, default="R")
    ap.add_argument("--cache", type=str, default="data/.fastf1_cache")
    ap.add_argument("--bronze", type=str, default="data/bronze")
    ap.add_argument("--out", type=str, default="data/silver")
    ap.add_argument("--rid", type=str, default=None)
    ap.add_argument("--rain-mm-threshold", type=float, default=RAIN_MM_THRESHOLD)
    args = ap.parse_args()
    run(args.season, args.event, args.kind, args.cache, args.bronze, args.out,
        args.rid, args.rain_mm_threshold)
