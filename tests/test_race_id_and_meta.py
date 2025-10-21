import os
from src.etl.bronze.utils import enable_cache, get_session, race_id
from src.etl.bronze.session_io import bronze_dir, save_meta
from pathlib import Path

def test_race_id_slug(tmp_path):
    enable_cache("data/.fastf1_cache")
    ses = get_session(2022, "Silverstone", "R")
    rid = race_id(ses)
    assert rid.startswith("2022_")
    assert " " not in rid and rid == rid.strip("_")

def test_meta_has_minimum_fields(tmp_path):
    enable_cache("data/.fastf1_cache")
    ses = get_session(2022, "Silverstone", "R")
    out = bronze_dir(tmp_path, race_id(ses))
    save_meta(ses, out)
    meta = (out / "meta.json").read_text()
    for key in ["season","event","location","race_id"]:
        assert key in meta
