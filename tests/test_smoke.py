from src.etl.bronze.utils import enable_cache
from src.etl.bronze.laps import extract_laps_df
from src.etl.bronze.weather import extract_weather_df
from src.etl.bronze.results import extract_results_df

def test_imports_ok():
    enable_cache("data/.fastf1_cache")
    assert callable(extract_laps_df)
    assert callable(extract_weather_df)
    assert callable(extract_results_df)
