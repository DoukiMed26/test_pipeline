# run.py — test_pipline (ETL callable par Airflow ou en CLI)
from __future__ import annotations
from pathlib import Path
import sys, json, csv
from argparse import ArgumentParser

# Localiser le projet (ce fichier est dans .../dags/projet/test_pipeline/)
BASE_DIR = Path(__file__).resolve().parent
SRC_DIR  = BASE_DIR / "src"          # contient le package Python "test_pipline"
DATA_DIR = BASE_DIR / "Data"         # fichiers d'entrée
OUT_DIR  = BASE_DIR / "outputs"      # sorties
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR.mkdir(parents=True, exist_ok=True)
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

# Import du code métier
from test_pipline.config import Config
from test_pipline.pipeline import run_pipeline

def extract() -> dict:
    """Vérifie la présence des fichiers d'entrée requis."""
    required = ["drugs.csv", "clinical_trials.csv"]
    missing = [n for n in required if not (DATA_DIR / n).exists()]
    has_pubmed = (DATA_DIR / "pubmed.csv").exists() or (DATA_DIR / "pubmed.json").exists()
    if missing or not has_pubmed:
        raise FileNotFoundError(
            f"Fichiers manquants: {missing}; pubmed présent ? {has_pubmed}. "
            f"Attendus dans {DATA_DIR}"
        )
    present = sorted(p.name for p in DATA_DIR.glob("*") if p.is_file())
    print(f"[extract] OK — présents: {present}")
    return {"present": present}

def transform(dayfirst: bool = True, generate_auto_id_if_empty: bool = True) -> dict:
    """Exécute la pipeline et produit outputs/drug_publications_by_atc.json"""
    cfg = Config(
        data_dir=DATA_DIR,
        out_dir=OUT_DIR,
        parse_dayfirst=dayfirst,
        generate_auto_id_if_empty=generate_auto_id_if_empty,
    )
    run_pipeline(cfg)
    out_file = OUT_DIR / "drug_publications_by_atc.json"
    if not out_file.exists():
        raise FileNotFoundError(out_file)
    print(f"[transform] JSON écrit -> {out_file}")
    return {"out_file": str(out_file)}

def load() -> dict:
    """Post-traitement : calcule le(s) journal(aux) citant le plus de médicaments distincts et exporte un CSV."""
    out_file = OUT_DIR / "drug_publications_by_atc.json"
    data = json.loads(out_file.read_text(encoding="utf-8"))

    journal_to_drugs: dict[str, set] = {}
    for atc, obj in data.items():
        for j in obj.get("journals", []) or []:
            name = j["journal"] if isinstance(j, dict) else j
            if name:
                journal_to_drugs.setdefault(name, set()).add(atc)

    rows = sorted(((j, len(s)) for j, s in journal_to_drugs.items()),
                  key=lambda x: (-x[1], x[0]))

    csv_path = OUT_DIR / "journal_drug_coverage.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["journal", "distinct_drugs"])
        w.writerows(rows)

    top = rows[0] if rows else ("N/A", 0)
    print(f"[load] Top journal: {top[0]} — {top[1]} médicaments distincts")
    return {"csv_path": str(csv_path), "top_journal": top[0], "count": top[1]}

# Option : exécution en CLI locale (python run.py [extract|transform|load|all])
if __name__ == "__main__":
    p = ArgumentParser()
    p.add_argument("step", nargs="?", choices=["extract","transform","load","all"], default="all")
    p.add_argument("--dayfirst", action="store_true")
    p.add_argument("--no-auto-id", action="store_true")
    args = p.parse_args()
    if args.step in ("extract","all"): extract()
    if args.step in ("transform","all"): transform(dayfirst=args.dayfirst, generate_auto_id_if_empty=not args.no_auto_id)
    if args.step in ("load","all"): load()
