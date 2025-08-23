# Lance la pipeline sans installer le package (ajoute src/ au sys.path).
import sys
from pathlib import Path
from argparse import ArgumentParser
sys.path.append(str(Path(__file__).resolve().parent / "src"))

from test_pipline.config import Config
from test_pipline.pipeline import run_pipeline

def main():
    p = ArgumentParser(description="test_pipline — ETL Drug→Publications")
    p.add_argument("--data-dir", type=Path, default=Path("Data"))
    p.add_argument("--out-dir",  type=Path, default=Path("outputs"))
    p.add_argument("--dayfirst", action="store_true", help="Dates EU (jour/mois/année)")
    p.add_argument("--no-auto-id", action="store_true", help="Ne pas générer d'ID auto si vide")
    args = p.parse_args()

    cfg = Config(
        data_dir=args.data_dir.resolve(),
        out_dir=args.out_dir.resolve(),
        parse_dayfirst=args.dayfirst or True,     # par défaut True : dates EU
        generate_auto_id_if_empty=(not args.no_auto_id)
    )
    run_pipeline(cfg)

if __name__ == "__main__":
    main()
