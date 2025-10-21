from __future__ import annotations
from pathlib import Path
import json
import numpy as np
import pandas as pd
from ..bronze.session_io import write_parquet
from .build import resolve_silver_dir, build_driver_strategy
from .quality import validate_strategy
from .schema import SCHEMA

def gold_dir(base="data/gold", rid:str|None=None) -> Path:
    p = Path(base) / (rid or "")
    p.mkdir(parents=True, exist_ok=True)
    return p

def _train_test_split(df: pd.DataFrame, seed: int = 42, train_ratio: float = 0.8):
    rng = np.random.default_rng(seed)
    mask = rng.random(len(df)) < train_ratio
    return df[mask].reset_index(drop=True), df[~mask].reset_index(drop=True)

def run(silver="data/silver", out="data/gold", rid: str | None = None,
        season: int | None = None, event: str | None = None, cache=None, kind="R",
        seed: int = 42, train_ratio: float = 0.8):
    # Resolución de sdir
    if rid is None:
        if season is None or event is None:
            raise ValueError("Proporcioná --rid o (--season y --event) para localizar Silver.")
        # si el usuario usa siempre YYYY_Silverstone, construimos rid:
        from ..bronze.utils import _slugify
        rid = f"{int(season)}_{_slugify(event)}"
    sdir = resolve_silver_dir(silver, rid)

    # Construcción
    ds = build_driver_strategy(sdir)
    validate_strategy(ds)

    # Guardar Gold + schema + splits
    gdir = gold_dir(out, rid)
    write_parquet(ds, gdir/"driver_strategy.parquet")
    (gdir/"schema.json").write_text(json.dumps(SCHEMA, indent=2, ensure_ascii=False))

    train, test = _train_test_split(ds, seed=seed, train_ratio=train_ratio)
    write_parquet(train, gdir/"train.parquet")
    write_parquet(test,  gdir/"test.parquet")

    print(f"GOLD listo → {gdir}")
    print(f"Rows: total={len(ds)} train={len(train)} test={len(test)}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--silver", type=str, default="data/silver")
    ap.add_argument("--out", type=str, default="data/gold")
    ap.add_argument("--rid", type=str, default=None)
    ap.add_argument("--season", type=int, default=None)
    ap.add_argument("--event", type=str, default=None)
    ap.add_argument("--kind", type=str, default="R")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--train-ratio", type=float, default=0.8)
    args = ap.parse_args()
    run(args.silver, args.out, args.rid, args.season, args.event, kind=args.kind,
        seed=args.seed, train_ratio=args.train_ratio)
