#!/usr/bin/env python3
"""Ad-hoc: trouver le(s) journal(aux) qui mentionne(nt) le plus de médicaments distincts
à partir du JSON produit par la pipeline (drug_publications_by_atc.json).
"""
import json
from pathlib import Path
from argparse import ArgumentParser

def load_by_atc(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Le JSON attendu est un dict indexé par codes ATC.")
    return data

def compute_counts(by_atc: dict) -> dict[str, set[str]]:
    journal_to_drugs: dict[str, set[str]] = {}
    for atc, obj in by_atc.items():
        for j in obj.get("journals", []) or []:
            if not j or not isinstance(j, dict):
                # compat si le JSON listait seulement les noms
                name = j if isinstance(j, str) else None
            else:
                name = j.get("journal")
            if not name:
                continue
            journal_to_drugs.setdefault(name, set()).add(atc)
    return journal_to_drugs

def main():
    p = ArgumentParser(description="Journal le plus couvrant (médicaments distincts)")
    p.add_argument("--input", type=Path, default=Path("outputs") / "drug_publications_by_atc.json",
                   help="Chemin vers le JSON produit par la pipeline")
    p.add_argument("--export-csv", type=Path, default=Path("outputs") / "journal_drug_coverage.csv",
                   help="Chemin CSV pour exporter les compteurs par journal")
    args = p.parse_args()

    by_atc = load_by_atc(args.input)
    journal_to_drugs = compute_counts(by_atc)

    rows = sorted(((j, len(s)) for j, s in journal_to_drugs.items()),
                  key=lambda x: (-x[1], x[0]))

    if not rows:
        print("Aucun journal trouvé dans l'entrée.")
        return

    top_count = rows[0][1]
    top_journals = [j for j, c in rows if c == top_count]
    print(f"Top journal(s): {top_journals} — {top_count} médicaments distincts")

    # Export CSV
    try:
        args.export_csv.parent.mkdir(parents=True, exist_ok=True)
        import csv
        with args.export_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["journal", "distinct_drugs"])
            for j, c in rows:
                w.writerow([j, c])
        print(f"[ok] CSV écrit → {args.export_csv}")
    except Exception as e:
        print(f"[warn] Impossible d'écrire le CSV: {e}")

if __name__ == "__main__":
    main()
