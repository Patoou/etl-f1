import pandas as pd
from src.etl.silver.normalize import compute_rain_flag

def test_rain_flag_threshold_applies():
    weather = pd.DataFrame({"rainfall": [0.0, 0.03, 0.049, 0.051]})
    laps = pd.DataFrame({"compound": ["S", "S", "M", "H"]})
    # umbral 0.05 → 0.051 dispara lluvia
    assert compute_rain_flag(weather, laps, mm_threshold=0.05) == 1
    # umbral 0.06 → nadie dispara
    assert compute_rain_flag(weather, laps, mm_threshold=0.06) == 0

def test_rain_flag_wet_compounds_override():
    weather = pd.DataFrame({"rainfall": [0.0, 0.0]})
    laps = pd.DataFrame({"compound": ["S", "W"]})
    assert compute_rain_flag(weather, laps, mm_threshold=10.0) == 1
