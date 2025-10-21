from __future__ import annotations
import pandas as pd
from typing import Optional

ALLOWED = {"S", "M", "H", "I", "W"}
RAIN_MM_THRESHOLD = 0.05  # mm por tick

def _td_to_ms(x) -> Optional[int]:
    if pd.isna(x):
        return None
    try:
        return int(pd.to_timedelta(x).total_seconds() * 1000)
    except Exception:
        return None

def canon_compound(x: str | None) -> Optional[str]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    t = str(x).strip().upper()
    if not t:
        return None
    if t.startswith("SOFT") or t == "S":
        return "S"
    if t.startswith("MEDIUM") or t == "M":
        return "M"
    if t.startswith("HARD") or t == "H":
        return "H"
    if t.startswith("INTER") or t == "I":
        return "I"
    if t.startswith("WET") or t == "W":
        return "W"
    if t in {"C1", "C2", "C3", "C4", "C5"}:
        return None
    return None

def normalize_laps(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "driver": df.get("Driver"),
        "driver_number": pd.to_numeric(df.get("DriverNumber"), errors="coerce").astype("Int64"),
        "team": df.get("Team"),
        "lap_number": pd.to_numeric(df.get("LapNumber"), errors="coerce").astype("Int64"),
        "stint_raw": pd.to_numeric(df.get("Stint"), errors="coerce").astype("Int64"),
        "compound": df.get("Compound").map(canon_compound),
        "track_status": df.get("TrackStatus"),
        "time_ms": df.get("Time").map(_td_to_ms).astype("Int64"),
        "pit_in_ms": df.get("PitInTime").map(_td_to_ms).astype("Int64"),
        "pit_out_ms": df.get("PitOutTime").map(_td_to_ms).astype("Int64"),
        "lap_time_ms": df.get("LapTime").map(_td_to_ms).astype("Int64"),
        "is_accurate": df.get("IsAccurate").astype("boolean").fillna(False).astype(bool),
        "is_pb": df.get("IsPersonalBest").astype("boolean").fillna(False).astype(bool),
    })
    # Dominio de compuestos
    out.loc[~out["compound"].isin(ALLOWED), "compound"] = None
    # Quitar filas sin driver o sin número de vuelta
    out = out.dropna(subset=["driver", "lap_number"])
    # Deduplicación estable (piloto + nro de vuelta)
    out = out.drop_duplicates(subset=["driver", "lap_number"], keep="first")
    # Orden canónico
    return out.sort_values(["driver", "lap_number"]).reset_index(drop=True)

def normalize_weather(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "time_ms": df.get("Time").map(_td_to_ms).astype("Int64"),
        "air_temp_c": pd.to_numeric(df.get("AirTemp"), errors="coerce"),
        "humidity": pd.to_numeric(df.get("Humidity"), errors="coerce"),
        "pressure": pd.to_numeric(df.get("Pressure"), errors="coerce"),
        "rainfall": pd.to_numeric(df.get("Rainfall"), errors="coerce").fillna(0.0),
        "track_temp_c": pd.to_numeric(df.get("TrackTemp"), errors="coerce"),
        "wind_dir": pd.to_numeric(df.get("WindDirection"), errors="coerce"),
        "wind_speed": pd.to_numeric(df.get("WindSpeed"), errors="coerce"),
    })
    return out.sort_values("time_ms").reset_index(drop=True)

def normalize_results(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "driver": df.get("Driver"),
        "driver_number": pd.to_numeric(df.get("DriverNumber"), errors="coerce").astype("Int64"),
        "team": df.get("Team"),
        "grid": pd.to_numeric(df.get("Grid"), errors="coerce").astype("Int64"),
        "final_pos": pd.to_numeric(df.get("Final"), errors="coerce").astype("Int64"),
        "finish_status": df.get("FinishStatus") if "FinishStatus" in df.columns else df.get("Status"),
    })
    return out.reset_index(drop=True)

def compute_rain_flag(weather_silver: pd.DataFrame,
                      laps_silver: pd.DataFrame,
                      mm_threshold: float = RAIN_MM_THRESHOLD) -> int:
    # Comp. de lluvia tienen prioridad
    if laps_silver["compound"].isin({"I", "W"}).any():
        return 1
    # Precipitación por encima del umbral
    if weather_silver["rainfall"].fillna(0).gt(mm_threshold).any():
        return 1
    return 0
