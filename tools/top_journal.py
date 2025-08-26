#!/usr/bin/env python3
from __future__ import annotations
import json
from pathlib import Path
from argparse import ArgumentParser
from typing import Dict, Set, List, Tuple

def load_by_atc(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Le JSON attendu est un dict indexé par codes ATC.")
    return data

def compute_counts(by_atc: dict) -> Dict[str, Set[str]]:
    journal_to_drugs: Dict[str, Set[str]] = {}
    for atc, obj in by_atc.items():
        for j in (obj.get("journals", []) or []):
            if isinstance(j, dict):
                name = j.get("journal")
            elif isinstance(j, str):
                name = j
            else:
                name = None
            if not name:
                continue
            journal_to_drugs.setdefault(name, set()).add(atc)
    return journal_to_drugs

def sort_rows(journal_to_drugs: Dict[str, Set[str]]) -> List[Tuple[str, int]]:
    return sorted(((j, len(s)) for j, s in journal_to_drugs.items()), key=lambda x: (-x[1], x[0]))

def main():
    p = ArgumentParser()
    p.add_argument("--input", type=Path, default=Path("outputs") / "drug_publications_by_atc.json")
    p.add_argument("--export-csv", type=Path, default=Path("outputs") / "journal_drug_coverage.csv")
    p.add_argument("--csv-mode", choices=["top1", "all"], default="top1")
    p.add_argument("--exclusive", action="store_true")
    args = p.parse_args()

    by_atc = load_by_atc(args.input)
    journal_to_drugs = compute_counts(by_atc)
    rows = sort_rows(journal_to_drugs)

    if not rows:
        print("Aucun journal trouvé")
        return

    top_journal, top_count = rows[0]
    print(f"Top journal: {top_journal} — {top_count} médicaments distincts")

    try:
        args.export_csv.parent.mkdir(parents=True, exist_ok=True)
        import csv
        mode = "x" if args.exclusive else "w"
        with args.export_csv.open(mode, newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["journal", "distinct_drugs"])
            if args.csv_mode == "top1":
                w.writerow([top_journal, top_count])
            else:
                for j, c in rows:
                    w.writerow([j, c])
        print(f"[ok] CSV écrit → {args.export_csv} ({args.csv_mode})")
    except FileExistsError:
        print(f"[warn] Le fichier existe déjà et --exclusive est activé: {args.export_csv}")
    except Exception as e:
        print(f"[warn] Impossible d'écrire le CSV: {e}")

if __name__ == "__main__":
    main()
