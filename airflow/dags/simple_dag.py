from pathlib import Path
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


RAW_BASE = "/opt/airflow/data/raw"


def validate_download(run_id: str, **_):
    folder = Path(f"{RAW_BASE}/{run_id}")
    files = [p for p in folder.glob("*") if p.is_file()]
    if not files:
        raise ValueError(f"No files downloaded in {folder}")
    print(f"Downloaded {len(files)} files to {folder}")


with DAG(
    dag_id="SP500_stock_data_download",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    schedule="@daily",
    catchup=False,
    tags=["kaggle", "smoke-test"],
) as dag:

    download = BashOperator(
        task_id="download_from_kaggle",
        bash_command=(
            "mkdir -p " + RAW_BASE + "/{{ run_id }} && "
            # Replace with your dataset slug:
            "kaggle datasets download "
            "-d {{ var.value.KAGGLE_DATASET_SLUG }} "
            "-p " + RAW_BASE + "/{{ run_id }} "
            "--unzip -q"
        ),
    )

    validate = PythonOperator(
        task_id="validate_download",
        python_callable=validate_download,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    download >> validate