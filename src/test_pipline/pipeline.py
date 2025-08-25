from __future__ import annotations
from datetime import datetime, timezone
import json, math
import pandas as pd
from .config import Config
from .io import read_drugs, read_pubmed, read_clinical_trials
from .clean import add_iso_date
from .match import build_patterns, find_mentions
from .aggregate import build_by_atc

def _clean_nans(obj):
    """Remplace tous les NaN/NaT/Inf par None dans une structure Python imbriquée."""
    import pandas as pd
    import math
    if obj is None:
        return None
    if isinstance(obj, float) and (math.isnan(obj) or obj in (float("inf"), float("-inf"))):
        return None
    if isinstance(obj, (pd.Timestamp,)):
        return obj.strftime("%Y-%m-%d")
    if isinstance(obj, dict):
        return {k: _clean_nans(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_nans(x) for x in obj]
    return obj

def run_pipeline(cfg: Config) -> None:
    """Orchestration :
    1) lecture des données → 2) dates ISO → 3) matching → 4) agrégation → 5) écriture JSON
    """
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    drugs = read_drugs(cfg.drugs_fp)
    pubmed = read_pubmed(cfg.pubmed_csv_fp, cfg.pubmed_json_fp)
    ctrials = read_clinical_trials(cfg.ctrials_fp)

    pubmed  = add_iso_date(pubmed,  dayfirst=cfg.parse_dayfirst)
    ctrials = add_iso_date(ctrials, dayfirst=cfg.parse_dayfirst)

    patterns = build_patterns(drugs)
    edges_pub = find_mentions(pubmed,  "pubmed",         patterns)
    edges_ct  = find_mentions(ctrials, "clinical_trial", patterns)
    edges = pd.concat([edges_pub, edges_ct], ignore_index=True)

    by_atc = build_by_atc(edges, generate_auto_id_if_empty=cfg.generate_auto_id_if_empty)
    payload = _clean_nans(by_atc)

    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")
    with cfg.by_atc_json_fp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, allow_nan=False)
    print(f"[ok] JSON écrit → {cfg.by_atc_json_fp}")
    print(f"[ok] generated_at = {generated_at}")
