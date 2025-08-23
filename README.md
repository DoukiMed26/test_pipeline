# test_pipline — ETL Drug → Publications

Projet Python **propre et modulaire** qui :
- lit `drugs.csv`, `pubmed.csv` + `pubmed.json`, `clinical_trials.csv` ;
- détecte les **mentions** de médicaments dans les **titres** ;
- produit un **JSON final groupé par code ATC** ;
- ajoute, pour chaque **journal**, la **première date**, la **dernière date** et le **nombre de publications distinctes** ;
- inclut un **traitement ad-hoc** pour trouver le journal qui mentionne le plus de **médicaments distincts**.

## Structure
```
test_pipline/
├─ src/
│  └─ test_pipline/
│     ├─ __init__.py
│     ├─ config.py
│     ├─ io.py
│     ├─ clean.py
│     ├─ match.py
│     ├─ aggregate.py
│     └─ pipeline.py
├─ tools/
│  └─ top_journal.py   # ad-hoc: top journal par nb de médicaments distincts
├─ Data/               # placez ici les fichiers fournis
├─ outputs/            # résultats
├─ run.py              # lance la pipeline sans installer le package
├─ requirements.txt
└─ README.md
```

## Entrées attendues (dans `Data/`)
- `drugs.csv` (colonnes : `atccode`, `drug`)
- `pubmed.csv` et/ou `pubmed.json` (colonnes : `id`, `title`, `journal`, `date`)
- `clinical_trials.csv` (colonnes : `id`, `scientific_title`→`title`, `journal`, `date`)

## Sorties (dans `outputs/`)
- `drug_publications_by_atc.json` : JSON final **par ATC** :
  ```json
  {
    "A04AD": {
      "drug": "DIPHENHYDRAMINE",
      "atccode": "A04AD",
      "pubmed": [
        {"id": 1, "title": "...", "date": "2019-01-01", "journal": "..."}
      ],
      "clinical_trials": [
        {"id": "NCT0123", "title": "...", "date": "2020-01-01", "journal": "..."}
      ],
      "journals": [
        {"journal": "Journal A", "first_date": "2019-01-01", "last_date": "2020-03-05", "n_pubs": 3}
      ]
    }
  }
  ```

## Installation
```bash
pip install -r requirements.txt
```

## Exécution de la pipeline
```bash
python run.py --data-dir Data --out-dir outputs --dayfirst
```
- `--dayfirst` : dates ambiguës interprétées **jour/mois/année** (EU). Retirez ce flag si vos sources sont US.

## Traitement ad-hoc (journal le plus couvrant)
Une fois le JSON généré :
```bash
python tools/top_journal.py --input outputs/drug_publications_by_atc.json --export-csv outputs/journal_drug_coverage.csv
```
