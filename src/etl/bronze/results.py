import pandas as pd

RES_COLS = ["Abbreviation","DriverNumber","TeamName","GridPosition","Position","Status"]

def _to_int_or_none(x):
    try:
        return int(x)
    except Exception:
        return None

def extract_results_df(session) -> pd.DataFrame:
    res = session.results.copy()
    cols = [c for c in RES_COLS if c in res.columns]
    res = res[cols].rename(columns={
        "Abbreviation":"Driver","TeamName":"Team",
        "GridPosition":"Grid","Position":"Final",
        "Status":"FinishStatus"
    })
    # normalizar Final (puede venir 'NC'/'DQ'); mantener FinishStatus textual
    res["Final"] = res["Final"].map(_to_int_or_none)
    return res.reset_index(drop=True)

def extract_pitstops_df(session) -> pd.DataFrame:
    # 1) intento API nativa
    try:
        ps = session.get_pit_stops()
        if ps is not None and len(ps):
            df = ps.copy().reset_index(drop=True)
            # asegurar DriverNumber si falta
            if "DriverNumber" not in df.columns:
                res = session.results[["Abbreviation","DriverNumber"]].copy()
                mapping = dict(zip(res["Abbreviation"], res["DriverNumber"]))
                if "Driver" in df.columns:
                    df["DriverNumber"] = df["Driver"].map(mapping)
            # columnas mínimas esperadas
            keep = [c for c in ["Driver","DriverNumber","Lap","Time","Duration","Stop"] if c in df.columns]
            return df[keep].rename(columns={"Lap":"LapNumber"})
    except Exception:
        pass

    # 2) fallback: derivar de laps
    laps = session.laps[["Driver","DriverNumber","LapNumber","PitInTime","PitOutTime"]].copy()
    mask = laps["PitInTime"].notna() | laps["PitOutTime"].notna()
    df = laps.loc[mask].copy()
    df["Stop"] = 1
    return df.reset_index(drop=True)

def save_results(session, outdir):
    from .session_io import write_parquet
    df = extract_results_df(session)
    write_parquet(df, outdir/"results_raw.parquet")
    ps = extract_pitstops_df(session)
    write_parquet(ps, outdir/"pitstops_raw.parquet")
    return df, ps
