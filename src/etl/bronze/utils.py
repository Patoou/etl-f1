from pathlib import Path
from tenacity import retry, wait_exponential, stop_after_attempt
import fastf1 as f1
import re, unicodedata
import pandas as pd

def enable_cache(path="data/.fastf1_cache"):
    p = Path(path); p.mkdir(parents=True, exist_ok=True)
    f1.Cache.enable_cache(p)

@retry(wait=wait_exponential(multiplier=1, min=3, max=60),
       stop=stop_after_attempt(6))
def get_session(season:int, event:str, kind:str="R"):
    ses = f1.get_session(season, event, kind)
    ses.load()
    return ses

def _slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_")
    return text

def _event_get(ev, key):
    try:
        if isinstance(ev, dict):
            return ev.get(key)
        if hasattr(ev, "get"):
            return ev.get(key, None)
        return getattr(ev, key, None)
    except Exception:
        try:
            return ev[key]
        except Exception:
            return None

def _coerce_year(val):
    if val is None:
        return None
    # int-like
    try:
        return int(val)
    except Exception:
        pass
    # datetime-like string/timestamp
    try:
        dt = pd.to_datetime(val, errors="coerce", utc=False)
        if pd.notna(dt):
            return int(dt.year)
    except Exception:
        pass
    # string with digits
    try:
        s = str(val)
        m = re.search(r"(19|20)\d{2}", s)
        if m:
            return int(m.group(0))
    except Exception:
        pass
    return None

def _event_year(ses) -> int:
    ev = getattr(ses, "event", None)

    # 1) keys numéricas
    for k in ("Year", "EventYear", "Season", "season", "year"):
        y = _coerce_year(_event_get(ev, k) if ev is not None else None)
        if y is not None:
            return y

    # 2) fechas dentro de event
    for k in ("EventDate", "Date", "SessionDate", "StartTime", "StartDate"):
        y = _coerce_year(_event_get(ev, k) if ev is not None else None)
        if y is not None:
            return y

    # 3) atributos del session
    for k in ("year", "season", "date", "start_time"):
        y = _coerce_year(getattr(ses, k, None))
        if y is not None:
            return y

    # 4) session_info dict
    si = getattr(ses, "session_info", None)
    if isinstance(si, dict):
        for k in ("Year", "EventYear", "Season", "EventDate", "Date"):
            y = _coerce_year(si.get(k))
            if y is not None:
                return y

    raise AttributeError("No se pudo resolver el año del evento")

def race_id(ses):
    ev = getattr(ses, "event", {})
    name = (_event_get(ev, "Location")
            or _event_get(ev, "EventName")
            or _event_get(ev, "OfficialEventName")
            or _event_get(ev, "Country")
            or "Unknown")
    year = _event_year(ses)
    return f"{year}_{_slugify(str(name))}"
