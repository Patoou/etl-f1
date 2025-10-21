import pandas as pd

LAP_COLS = [
    "Time","Driver","DriverNumber","Team","LapNumber","Stint",
    "Compound","TrackStatus","PitInTime","PitOutTime","LapTime",
    "IsAccurate","IsPersonalBest"
]

def extract_laps_df(session) -> pd.DataFrame:
    df = session.laps.copy()
    cols = [c for c in LAP_COLS if c in df.columns]
    return df[cols].reset_index(drop=True)

def save_laps(session, outdir):
    from .session_io import write_parquet
    df = extract_laps_df(session)
    write_parquet(df, outdir/"laps_raw.parquet")
    return df
