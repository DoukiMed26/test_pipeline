from __future__ import annotations
from pathlib import Path
import json, re
import pandas as pd

def load_lenient_json_list(path: Path) -> list[dict]:
    """Charge un JSON possiblement mal formé et renvoie une liste de dicts.
    - Gère BOM, virgules finales, et un seul objet non listé.
    - Fallback: NDJSON ligne par ligne.
    """
    if not path.exists():
        return []
    raw = path.read_text(encoding="utf-8").strip().lstrip("\ufeff")
    raw = re.sub(r",\s*([\]}])", r"\1", raw)  
    if not raw.startswith("["):
        raw = "[" + raw + "]"                
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        items: list[dict] = []
        for line in raw.splitlines():
            line = line.strip().rstrip(",")
            if not line or line in ("[", "]"):
                continue
            try:
                items.append(json.loads(line))
            except Exception:
                pass
        return items

def read_drugs(fp: Path) -> pd.DataFrame:
    """Lit drugs.csv et normalise les champs de base."""
    df = pd.read_csv(fp)
    return (df.assign(
                drug=lambda d: d["drug"].astype(str).str.strip(),
                atccode=lambda d: d["atccode"].astype(str).str.strip()
            )[["atccode","drug"]]
    )

def read_pubmed(csv_fp: Path, json_fp: Path) -> pd.DataFrame:
    """Lit pubmed.csv et/ou pubmed.json et harmonise le schéma."""
    parts = []
    if csv_fp.exists():
        parts.append(pd.read_csv(csv_fp))
    if json_fp.exists():
        lst = load_lenient_json_list(json_fp)
        if lst:
            parts.append(pd.DataFrame(lst))
    if parts:
        df = pd.concat(parts, ignore_index=True)
    else:
        df = pd.DataFrame(columns=["id","title","journal","date"])
    df = df.rename(columns={"id":"id","title":"title","journal":"journal","date":"date"})
    return df[["id","title","journal","date"]]

def read_clinical_trials(fp: Path) -> pd.DataFrame:
    """Lit clinical_trials.csv et renomme scientific_title -> title."""
    df = pd.read_csv(fp).rename(columns={"scientific_title":"title"})
    return df[["id","title","journal","date"]]
