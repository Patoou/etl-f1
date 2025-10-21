from .utils import enable_cache, get_session, race_id
from .session_io import bronze_dir, save_meta
from .laps import save_laps
from .weather import save_weather
from .results import save_results
import argparse

def run(season:int, event:str, kind:str="R", cache="data/.fastf1_cache", out="data/bronze"):
    enable_cache(cache)
    ses = get_session(season, event, kind)
    rid = race_id(ses)
    outdir = bronze_dir(out, rid)
    save_meta(ses, outdir)
    save_laps(ses, outdir)
    save_weather(ses, outdir)
    save_results(ses, outdir)
    print(f"BRONZE listo  {outdir}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--event", type=str, required=True)
    ap.add_argument("--kind", type=str, default="R")
    ap.add_argument("--cache", type=str, default="data/.fastf1_cache")
    ap.add_argument("--out", type=str, default="data/bronze")
    args = ap.parse_args()
    run(args.season, args.event, args.kind, args.cache, args.out)
