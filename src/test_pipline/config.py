from dataclasses import dataclass
from pathlib import Path

@dataclass
class Config:
    """Configuration simple de la pipeline."""
    data_dir: Path
    out_dir: Path
    parse_dayfirst: bool = True                  # Interpréter dates ambiguës en mode EU
    generate_auto_id_if_empty: bool = True       # Générer un ID si publication sans ID

    @property
    def drugs_fp(self) -> Path: return self.data_dir / "drugs.csv"
    @property
    def pubmed_csv_fp(self) -> Path: return self.data_dir / "pubmed.csv"
    @property
    def pubmed_json_fp(self) -> Path: return self.data_dir / "pubmed.json"
    @property
    def ctrials_fp(self) -> Path: return self.data_dir / "clinical_trials.csv"
    @property
    def by_atc_json_fp(self) -> Path: return self.out_dir / "drug_publications_by_atc.json"
