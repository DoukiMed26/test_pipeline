from __future__ import annotations
import re, unicodedata, hashlib
import pandas as pd

_HEXSEQ = re.compile(r"(?:\\x[0-9A-Fa-f]{2})+")

def fix_mojibake(s: str | None) -> str | None:
    r"""Corrige séquences '\\xNN' et normalise en NFKC (évite doublons de journaux)."""
    if s is None: return None
    s = str(s)
    def _dec(m):
        bs = bytes(int(h,16) for h in re.findall(r"\\x([0-9A-Fa-f]{2})", m.group(0)))
        try: return bs.decode("utf-8")
        except UnicodeDecodeError: return ""
    s = _HEXSEQ.sub(_dec, s)
    return unicodedata.normalize("NFKC", s).strip()

def add_iso_date(df: pd.DataFrame, dayfirst: bool) -> pd.DataFrame:
    """Ajoute une colonne 'date_iso' (YYYY-MM-DD) après parsing robuste."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], dayfirst=dayfirst, errors="coerce")
    df["date_iso"] = df["date"].dt.strftime("%Y-%m-%d")
    return df

def safe_date(v) -> str | None:
    """Retourne une date ISO ou None."""
    if pd.isna(v) or v in ("", "NaT", None): return None
    ts = pd.to_datetime(v, errors="coerce")
    return None if pd.isna(ts) else ts.strftime("%Y-%m-%d")

def safe_id(v):
    """int si numérique, sinon str, sinon None."""
    if v is None: return None
    s = str(v).strip()
    if not s: return None
    return int(s) if s.isdigit() else s

def make_pub_id(title: str, journal: str, date_iso: str | None) -> str:
    """ID stable auto (hash) si publication sans identifiant."""
    base = f"{title}|{journal}|{date_iso or ''}"
    return "AUTO_" + hashlib.md5(base.encode("utf-8")).hexdigest()[:10]
