from src.etl.bronze.utils import enable_cache, get_session
from src.etl.bronze.laps import extract_laps_df
from src.etl.bronze.weather import extract_weather_df

def test_connect_and_extract(tmp_path):
    enable_cache(tmp_path/".cache")
    s = get_session(2022, "Silverstone", "R")
    assert len(extract_laps_df(s)) > 10
    assert "Time" in extract_weather_df(s).columns
