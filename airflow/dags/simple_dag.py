from pathlib import Path
import pendulum
from google.cloud import storage

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
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

    # Download dataset from Kaggle
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

    # Validate the download
    validate = PythonOperator(
        task_id="validate_download",
        python_callable=validate_download,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    # Upload to GCS
    upload = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "run_id": "{{ run_id }}",
            "ds": "{{ ds }}",
            "bucket_name": "{{ var.value.GCS_BUCKET_NAME }}"
        }
    )

    # Create BQ tables and load data
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

    # Load raw data into BQ
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

    # Load companies into BQ
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

    # Load index into BQ
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

    # Transform raw data into mart table
    transform_mart = BigQueryInsertJobOperator(
        task_id="transform_mart",
        configuration={
                # CREATE OR REPLACE TABLE `{{ var.value.GCP_PROJECT_ID }}.{{ var.value.BQ_DATASET }}.mart_sector_daily`
            "query": {
                "query": """
                DROP TABLE IF EXISTS `{{ var.value.GCP_PROJECT_ID }}.{{ var.value.BQ_DATASET }}.mart_sector_daily`;
                CREATE TABLE `{{ var.value.GCP_PROJECT_ID }}.{{ var.value.BQ_DATASET }}.mart_sector_daily`
                PARTITION BY date
                CLUSTER BY sector AS
                SELECT
                DATE(s.Date) AS date,
                c.Sector AS sector,
                AVG(CAST(s.Close AS FLOAT64)) AS avg_close,
                SUM(CAST(s.Volume AS FLOAT64)) AS total_volume,
                COUNT(DISTINCT s.Symbol) AS company_count
                FROM `{{ var.value.GCP_PROJECT_ID }}.{{ var.value.BQ_DATASET }}.raw_sp500_stocks` s
                JOIN `{{ var.value.GCP_PROJECT_ID }}.{{ var.value.BQ_DATASET }}.raw_sp500_companies` c
                ON s.Symbol = c.Symbol
                GROUP BY date, sector
                """,
                "useLegacySql": False,
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
    
    download >> validate >> upload >> create_mart_table >> [load_companies, load_stocks, load_index] >> transform_mart >> check_mart_rows