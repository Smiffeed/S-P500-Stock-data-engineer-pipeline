from pathlib import Path
import pendulum
from google.cloud import storage

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


RAW_BASE = "/opt/airflow/data/raw"


def validate_download(run_id: str, **_):
    folder = Path(f"{RAW_BASE}/{run_id}")
    files = [p for p in folder.glob("*") if p.is_file()]
    if not files:
        raise ValueError(f"No files downloaded in {folder}")
    print(f"Downloaded {len(files)} files to {folder}")

def upload_to_gcs(run_id: str, ds: str, bucket_name: str, **_):
    folder = Path(f"{RAW_BASE}/{run_id}")
    files = [p for p in folder.glob("*") if p.is_file()]
    if not files:
        raise ValueError(f"No files to upload in {folder}")
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    for f in files:
        blob_path = f"raw/{ds}/{f.name}"
        bucket.blob(blob_path).upload_from_filename(str(f))
        print(f"Uploaded {f.name} → gs://{bucket_name}/{blob_path}")


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

    upload = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "run_id": "{{ run_id }}",
            "ds": "{{ ds }}",
            "bucket_name": "{{ var.value.GCS_BUCKET_NAME }}"
        }
    )

    download >> validate >> upload