from pathlib import Path
import shutil

import pendulum
from google.cloud import storage

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


RAW_BASE = "/opt/airflow/data/raw"
UPLOAD_BASE = "/opt/airflow/upload"


def extract_from_upload(run_id: str, **_):
    source_folder = Path(UPLOAD_BASE)
    if not source_folder.exists() or not source_folder.is_dir():
        raise ValueError(f"Upload folder not found: {source_folder}")

    source_files = [p for p in source_folder.glob("*.csv") if p.is_file()]
    if not source_files:
        raise ValueError(f"No CSV files found in {source_folder}")

    target_folder = Path(f"{RAW_BASE}/{run_id}")
    target_folder.mkdir(parents=True, exist_ok=True)

    for f in source_files:
        shutil.copy2(f, target_folder / f.name)
    print(f"Copied {len(source_files)} file(s) to {target_folder}")


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
        print(f"Uploaded {f.name} -> gs://{bucket_name}/{blob_path}")


with DAG(
    dag_id="SP500_stock_data_upload_from_local_csv",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["upload", "smoke-test"],
) as dag:

    download = PythonOperator(
        task_id="extract_from_upload",
        python_callable=extract_from_upload,
        op_kwargs={"run_id": "{{ run_id }}"},
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
            "bucket_name": "{{ var.value.GCS_BUCKET_NAME }}",
        }
    )

    create_mart_table = BigQueryInsertJobOperator(
        task_id="create_mart_table",
        configuration={
            "query": {
                "query": """
                CREATE TABLE IF NOT EXISTS `{{ var.value.GCP_PROJECT_ID }}.{{ var.value.BQ_DATASET }}.mart_sector_daily`
                (
                date DATE,
                sector STRING,
                avg_close FLOAT64,
                total_volume FLOAT64,
                company_count INT64
                )
                PARTITION BY date
                CLUSTER BY sector
                """,
                "useLegacySql": False,
            }
        },
        location="asia-southeast3",
    )

    load_stocks = BigQueryInsertJobOperator(
        task_id="load_stocks",
        configuration={
            "load": {
                "sourceUris": ["gs://{{ var.value.GCS_BUCKET_NAME }}/raw/{{ ds }}/sp500_stocks.csv"],
                "destinationTable": {
                    "projectId": "{{ var.value.GCP_PROJECT_ID }}",
                    "datasetId": "{{ var.value.BQ_DATASET }}",
                    "tableId": "raw_sp500_stocks",
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "schema": {
                    "fields": [
                        {"name": "Date", "type": "DATE"},
                        {"name": "Symbol", "type": "STRING"},
                        {"name": "Adj Close", "type": "FLOAT64"},
                        {"name": "Close", "type": "FLOAT64"},
                        {"name": "High", "type": "FLOAT64"},
                        {"name": "Low", "type": "FLOAT64"},
                        {"name": "Open", "type": "FLOAT64"},
                        {"name": "Volume", "type": "FLOAT64"},
                    ]
                },
                "maxBadRecords": 10,
                "allowQuotedNewlines": True,
                "ignoreUnknownValues": True,
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        location="asia-southeast3",
    )

    load_companies = BigQueryInsertJobOperator(
        task_id="load_companies",
        configuration={
            "load": {
                "sourceUris": ["gs://{{ var.value.GCS_BUCKET_NAME }}/raw/{{ ds }}/sp500_companies.csv"],
                "destinationTable": {
                    "projectId": "{{ var.value.GCP_PROJECT_ID }}",
                    "datasetId": "{{ var.value.BQ_DATASET }}",
                    "tableId": "raw_sp500_companies",
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "autodetect": True,
                "maxBadRecords": 10,
                "allowQuotedNewlines": True,
                "ignoreUnknownValues": True,
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        location="asia-southeast3",
    )

    load_index = BigQueryInsertJobOperator(
        task_id="load_index",
        configuration={
            "load": {
                "sourceUris": ["gs://{{ var.value.GCS_BUCKET_NAME }}/raw/{{ ds }}/sp500_index.csv"],
                "destinationTable": {
                    "projectId": "{{ var.value.GCP_PROJECT_ID }}",
                    "datasetId": "{{ var.value.BQ_DATASET }}",
                    "tableId": "raw_sp500_index",
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "autodetect": True,
                "maxBadRecords": 10,
                "allowQuotedNewlines": True,
                "ignoreUnknownValues": True,
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        location="asia-southeast3",
    )

    load_mart_from_spark = BigQueryInsertJobOperator(
        task_id="load_mart_from_spark",
        configuration={
            "load": {
                "sourceUris": ["gs://{{ var.value.GCS_BUCKET_NAME }}/processed/sp500_sector_daily/*"],
                "destinationTable": {
                    "projectId": "{{ var.value.GCP_PROJECT_ID }}",
                    "datasetId": "{{ var.value.BQ_DATASET }}",
                    "tableId": "mart_sector_daily",
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
            }
        },
        location="asia-southeast3",
    )

    check_mart_rows = BigQueryCheckOperator(
        task_id="check_mart_rows",
        sql="""
        SELECT COUNT(*) > 0
        FROM `{{ var.value.GCP_PROJECT_ID }}.{{ var.value.BQ_DATASET }}.mart_sector_daily`
        """,
        use_legacy_sql=False,
        location="asia-southeast3",
    )

    spark_transform = SparkSubmitOperator(
        task_id="spark_transform_sp500",
        application="/opt/airflow/dags/scripts/transform_sp500.py",
        name="sp500-transform",
        conn_id="spark_default",
        application_args=[
            "--bucket",
            "{{ var.value.GCS_BUCKET_NAME }}",
            "--raw-prefix",
            "raw",
            "--output-prefix",
            "processed/sp500_sector_daily",
        ],
        verbose=True,
        env_vars={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/spark/config/gcp_credentials.json",
        },
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "client",
            "spark.driver.host": "airflow-worker",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.jars.ivy": "/tmp/.ivy2",
            "spark.jars.packages": "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.26",
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.google.cloud.auth.service.account.enable": "true",
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile": "/opt/spark/config/gcp_credentials.json",
            "spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS": "/opt/spark/config/gcp_credentials.json",
        },
    )

    download >> validate >> upload
    upload >> [load_companies, load_stocks, load_index, spark_transform]
    spark_transform >> create_mart_table >> load_mart_from_spark >> check_mart_rows
