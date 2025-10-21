#!/usr/bin/env python
from __future__ import annotations
import hashlib, json, os, time
from pathlib import Path

ROOTS = [Path("data/gold"), Path("data/silver")]
OUT_TXT = Path("data/CHECKSUMS.txt")
OUT_JSON = Path("data/VERSION.json")

def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1<<20), b""):
            h.update(chunk)
    return h.hexdigest()

def main():
    files = []
    for root in ROOTS:
        if not root.exists(): continue
        for p in root.rglob("*"):
            if p.is_file():
                files.append(p)

    lines = []
    for p in sorted(files):
        digest = sha256_of(p)
        rel = p.as_posix()
        lines.append(f"{digest}  {rel}")

    OUT_TXT.parent.mkdir(parents=True, exist_ok=True)
    OUT_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")

    seasons = sorted({int(p.name.split("_",1)[0]) for p in Path("data/gold").glob("*_*") if p.is_dir()})
    meta = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "seasons": seasons,
        "file_count": len(files),
        "checksums_path": OUT_TXT.as_posix()
    }
    OUT_JSON.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"Escribí {OUT_TXT} y {OUT_JSON}")

if __name__ == "__main__":
    main()
