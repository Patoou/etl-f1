from pathlib import Path
import json

def bronze_dir(base="data/bronze", rid: str | None = None) -> Path:
    p = Path(base) / (rid or "")
    p.mkdir(parents=True, exist_ok=True)
    return p

def write_parquet(df, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Fuerza engine y compresión consistente
    df.to_parquet(path, index=False, engine="pyarrow", compression="snappy")

def save_meta(session, outdir: Path):
    meta = {
        "season": int(session.event.year),
        "event": session.event.get("EventName"),
        "official_event": session.event.get("OfficialEventName"),
        "country": session.event.get("Country"),
        "location": session.event.get("Location"),
        "round": int(session.event.get("RoundNumber", 0)),
        "total_laps": int(getattr(session, "total_laps", 0) or 0),
        "race_id": outdir.name,
    }
    (outdir / "meta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False))
