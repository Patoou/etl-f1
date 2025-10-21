import pandas as pd
WEA_COLS = ["Time","AirTemp","Humidity","Pressure","Rainfall","TrackTemp","WindDirection","WindSpeed"]

def extract_weather_df(session) -> pd.DataFrame:
    wd = session.weather_data.copy()
    cols = [c for c in WEA_COLS if c in wd.columns]
    return wd[cols].reset_index(drop=True)

def save_weather(session, outdir):
    from .session_io import write_parquet
    df = extract_weather_df(session)
    write_parquet(df, outdir/"weather_raw.parquet")
    return df
