from __future__ import annotations
import re
import pandas as pd

def build_drug_pattern(name: str) -> re.Pattern:
    """Regex 'mot entier' insensible à la casse pour un nom de médicament."""
    esc = re.escape(str(name))
    return re.compile(rf"\b{esc}\b", flags=re.IGNORECASE)

def build_patterns(drugs: pd.DataFrame):
    """Construit la liste (atccode, drug, pattern) pour tous les médicaments."""
    return [(r["atccode"], r["drug"], build_drug_pattern(r["drug"])) for _, r in drugs.iterrows()]

def find_mentions(df: pd.DataFrame, source: str, drug_patterns) -> pd.DataFrame:
    """Renvoie un DataFrame 'edges' : une ligne par mention (médicament trouvé dans le titre).
    Colonnes : drug_atccode, drug_name, source, pub_id, title, journal, date
    """
    records = []
    for row in df.itertuples(index=False):
        title = str(getattr(row, "title", "")) if pd.notnull(getattr(row, "title", "")) else ""
        journal = str(getattr(row, "journal", "")) if pd.notnull(getattr(row, "journal", "")) else ""
        pid = getattr(row, "id", None)
        date_iso = getattr(row, "date_iso", None)
        for atccode, drug_name, pat in drug_patterns:
            if pat.search(title):
                records.append({
                    "drug_atccode": str(atccode),
                    "drug_name": str(drug_name),
                    "source": source,
                    "pub_id": None if pd.isna(pid) else str(pid).strip(),
                    "title": title,
                    "journal": journal,
                    "date": date_iso
                })
    return pd.DataFrame.from_records(records)
