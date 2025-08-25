# dag.py — etl_drug_mentions : 3 tasks (extract -> transform -> load)
from __future__ import annotations
import importlib.util, sys
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_ID  = "etl_drug_mentions"
DAG_DIR = Path(__file__).resolve().parent
RUN_PY  = DAG_DIR / "run.py"   

def _load_run_module():
    """Charge run.py par chemin et renvoie le module importé."""
    if not RUN_PY.exists():
        raise FileNotFoundError(f"run.py introuvable: {RUN_PY}")
    spec = importlib.util.spec_from_file_location("run_module", str(RUN_PY))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["run_module"] = mod
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod

def py_extract(**_):
    mod = _load_run_module()
    return mod.extract()

def py_transform(**_):
    mod = _load_run_module()
    # dayfirst=True : format EU (jour/mois/année) ; change à False si sources US
    return mod.transform(dayfirst=True, generate_auto_id_if_empty=True)

def py_load(**_):
    mod = _load_run_module()
    return mod.load()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    description="ETL test_pipline (extract/transform/load)",
    schedule="@daily",                 # ou None pour déclencher à la demande
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "test_pipline"],
) as dag:

    extract_task   = PythonOperator(task_id="extract_data",   python_callable=py_extract)
    transform_task = PythonOperator(task_id="transform_data", python_callable=py_transform)
    load_task      = PythonOperator(task_id="load_data",      python_callable=py_load)

    extract_task >> transform_task >> load_task
