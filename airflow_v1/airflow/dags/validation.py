import os
import socket
from urllib.parse import urlparse

import pendulum
from airflow import DAG
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk import Variable
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.standard.operators.python import PythonOperator
from google.cloud import bigquery
from google.cloud import storage
from kaggle.api.kaggle_api_extended import KaggleApi


def check_required_variables():
    required_vars = ["GCS_BUCKET_NAME", "GCP_PROJECT_ID", "BQ_DATASET"]
    missing_vars = [var for var in required_vars if not Variable.get(var, default=None)]
    if missing_vars:
        raise ValueError(f"Missing required Airflow Variables: {', '.join(missing_vars)}")


def check_airflow_connections():
    spark_conn = BaseHook.get_connection("spark_default")
    gcp_conn = BaseHook.get_connection("google_cloud_default")

    if spark_conn.conn_type != "spark":
        raise ValueError(
            f"Connection spark_default has wrong conn_type '{spark_conn.conn_type}', expected 'spark'"
        )
    if gcp_conn.conn_type != "google_cloud_platform":
        raise ValueError(
            "Connection google_cloud_default has wrong conn_type "
            f"'{gcp_conn.conn_type}', expected 'google_cloud_platform'"
        )

    expected_project = Variable.get("GCP_PROJECT_ID")
    conn_project = (
        gcp_conn.extra_dejson.get("project")
        or gcp_conn.extra_dejson.get("extra__google_cloud_platform__project")
    )
    if conn_project and conn_project != expected_project:
        raise ValueError(
            "google_cloud_default project mismatch: "
            f"connection project='{conn_project}', variable GCP_PROJECT_ID='{expected_project}'"
        )


def _get_google_credentials_and_project():
    expected_project = Variable.get("GCP_PROJECT_ID")
    gcp_hook = GoogleBaseHook(gcp_conn_id="google_cloud_default")
    credentials, conn_project = gcp_hook.get_credentials_and_project_id()

    if conn_project and conn_project != expected_project:
        raise ValueError(
            "google_cloud_default resolved project does not match GCP_PROJECT_ID: "
            f"connection project='{conn_project}', variable GCP_PROJECT_ID='{expected_project}'"
        )

    return credentials, (conn_project or expected_project)


def check_gcp_access():
    bucket_name = Variable.get("GCS_BUCKET_NAME")
    credentials, project_id = _get_google_credentials_and_project()

    client = storage.Client(project=project_id, credentials=credentials)
    bucket = client.bucket(bucket_name)
    if not bucket.exists():
        raise ValueError(f"GCS bucket not found or inaccessible: {bucket_name}")


def check_bigquery_access():
    credentials, project_id = _get_google_credentials_and_project()
    dataset_id = Variable.get("BQ_DATASET")
    dataset_ref = f"{project_id}.{dataset_id}"

    client = bigquery.Client(project=project_id, credentials=credentials)
    try:
        client.get_dataset(dataset_ref)
    except Exception as exc:
        raise ValueError(
            f"BigQuery dataset is not accessible: {dataset_ref}. Original error: {exc}"
        ) from exc


def check_kaggle_access():
    if not os.environ.get("KAGGLE_USERNAME") or not os.environ.get("KAGGLE_KEY"):
        raise ValueError("KAGGLE_USERNAME and/or KAGGLE_KEY env vars are missing")

    dataset_slug = Variable.get("KAGGLE_DATASET_SLUG")
    api = KaggleApi()
    api.authenticate()
    api.validate_dataset_string(dataset_slug)
    try:
        api.dataset_list_files(dataset_slug)
    except Exception as exc:
        raise ValueError(
            f"Kaggle dataset is not accessible: {dataset_slug}. Original error: {exc}"
        ) from exc


def check_spark_reachable():
    spark_conn = BaseHook.get_connection("spark_default")
    raw_host = spark_conn.host or "spark-master"
    port = spark_conn.port or 7077

    # Some Airflow Spark connections store host as spark://spark-master.
    if "://" in raw_host:
        parsed = urlparse(raw_host)
        host = parsed.hostname or raw_host
        if parsed.port:
            port = parsed.port
    else:
        host = raw_host

    try:
        with socket.create_connection((host, int(port)), timeout=10):
            pass
    except OSError as exc:
        raise ValueError(
            f"Spark endpoint is not reachable: host={host}, port={port}, conn_id=spark_default. "
            f"Original error: {exc}"
        ) from exc


with DAG(
    dag_id="validation_variables_and_connections",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["smoke-test", "validation"],
) as dag:
    check_vars = PythonOperator(
        task_id="check_required_variables",
        python_callable=check_required_variables,
    )

    check_conns = PythonOperator(
        task_id="check_airflow_connections",
        python_callable=check_airflow_connections,
    )

    check_gcp = PythonOperator(
        task_id="check_gcp_access",
        python_callable=check_gcp_access,
    )

    check_bigquery = PythonOperator(
        task_id="check_bigquery_access",
        python_callable=check_bigquery_access,
    )

    check_kaggle = PythonOperator(
        task_id="check_kaggle_access",
        python_callable=check_kaggle_access,
    )

    check_spark = PythonOperator(
        task_id="check_spark_reachable",
        python_callable=check_spark_reachable,
    )

    check_vars >> check_conns >> [check_gcp, check_bigquery, check_kaggle, check_spark]