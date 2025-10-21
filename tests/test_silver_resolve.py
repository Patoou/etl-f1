import os
import pytest
from pathlib import Path
from src.etl.silver.run import resolve_bronze_dir
from src.etl.bronze.utils import _slugify

class FakeSes:
    def __init__(self, year, event_dict):
        self.event = {"year": year, **event_dict}

def test_resolve_prefiere_location(tmp_path):
    base = tmp_path / "bronze"; base.mkdir(parents=True)
    (base / "2022_Silverstone").mkdir()
    ses = FakeSes(2022, {"Location": "Silverstone", "EventName": "British Grand Prix"})
    got = resolve_bronze_dir(str(base), ses, None)
    assert got.name == "2022_Silverstone"

def test_resolve_cae_a_eventname(tmp_path):
    base = tmp_path / "bronze"; base.mkdir(parents=True)
    slug = _slugify("British Grand Prix")
    (base / f"2022_{slug}").mkdir()
    ses = FakeSes(2022, {"Location": "SomeOther", "EventName": "British Grand Prix"})
    got = resolve_bronze_dir(str(base), ses, None)
    assert got.name == f"2022_{slug}"
