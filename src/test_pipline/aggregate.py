from __future__ import annotations
from typing import Dict, Any, List
import pandas as pd
from .clean import fix_mojibake, safe_date, safe_id, make_pub_id

def _journal_summary(g: pd.DataFrame) -> List[dict]:
    """Pour un groupe (médicament), résume les journaux avec date et compte :
    - first_date : première date observée pour ce (journal, médicament)
    - last_date  : dernière date observée
    - n_pubs     : nb de publications DISTINCTES (pub_id) pour ce couple
    """
    df = g.dropna(subset=["journal"]).copy()
    df["journal"] = df["journal"].map(lambda x: x.strip() if isinstance(x, str) else None)
    df["journal"] = df["journal"].map(fix_mojibake)
    df["date_parsed"] = pd.to_datetime(df["date"], errors="coerce")

    agg = (df.groupby("journal", as_index=False)
             .agg(first_date=("date_parsed","min"),
                  last_date =("date_parsed","max"),
                  n_pubs    =("pub_id","nunique")))

    # Convertir en texte ISO et forcer None si manquant (évite NaN/NaT dans le JSON)
    for col in ("first_date", "last_date"):
        s = agg[col].dt.strftime("%Y-%m-%d")
        agg[col] = s.where(s.notna(), None)

    # n_pubs en int Python
    agg["n_pubs"] = agg["n_pubs"].fillna(0).astype(int)

    agg = agg.sort_values("journal", kind="stable")

    out: List[dict] = []
    for r in agg.itertuples(index=False):
        out.append({
            "journal": r.journal,
            "first_date": r.first_date,   # str "YYYY-MM-DD" ou None
            "last_date": r.last_date,     # str "YYYY-MM-DD" ou None
            "n_pubs": int(r.n_pubs)
        })
    return out

def build_by_atc(edges: pd.DataFrame, generate_auto_id_if_empty: bool) -> Dict[str, Any]:
    """Construit le JSON final groupé par ATC.
    - 'pubmed' et 'clinical_trials' : listes d'objets {id, title, date, journal}
    - 'journals' : liste d'objets {journal, first_date, last_date, n_pubs}
    """
    e = edges.copy()
    for col in ("journal","title"):
        e[col] = e[col].map(fix_mojibake)
    e["date_parsed"] = pd.to_datetime(e["date"], errors="coerce")

    out: Dict[str, Any] = {}
    for (atc, drug), g in e.groupby(["drug_atccode","drug_name"], dropna=False):
        g = g.sort_values("date_parsed", na_position="last")

        def rows_for(src: str):
            items = []
            gf = g[g["source"].eq(src)]
            for r in gf.itertuples(index=False):
                rid = safe_id(r.pub_id)
                rdate = safe_date(r.date)
                # Normaliser journal/title pour éviter des NaN JSON
                rjournal = r.journal if (r.journal is not None and not (isinstance(r.journal,float) and pd.isna(r.journal))) else None
                rtitle = r.title if (r.title is not None and not (isinstance(r.title,float) and pd.isna(r.title))) else ""
                if rid is None and generate_auto_id_if_empty:
                    rid = make_pub_id(rtitle, rjournal or "", rdate)
                items.append({"id": rid, "title": rtitle, "date": rdate, "journal": rjournal})
            return items

        pubmed = rows_for("pubmed")
        clinical = rows_for("clinical_trial")
        journals = _journal_summary(g)

        out[str(atc)] = {
            "drug": str(drug) if drug is not None else None,
            "atccode": str(atc) if atc is not None else None,
            "pubmed": pubmed,
            "clinical_trials": clinical,
            "journals": journals
        }
    return out
