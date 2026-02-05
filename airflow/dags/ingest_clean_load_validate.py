"""
Week 8 Buổi 15 — DAG: ingest → clean → load → validate.
Pipeline: Extract từ Postgres → Clean (drop null, chuẩn hóa) → Load lên MinIO → Validate (file tồn tại, row count).
"""
from datetime import datetime, timedelta
import os
import csv
import logging

import psycopg2
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------

POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "de_db"
POSTGRES_USER = "de_user"
POSTGRES_PASSWORD = "de_pass"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "lakehouse"
MINIO_KEY = "bronze/orders/orders_clean.csv"

LOCAL_TMP_DIR = "/tmp/de_etl"
LOCAL_RAW_PATH = os.path.join(LOCAL_TMP_DIR, "orders_raw.csv")
LOCAL_CLEAN_PATH = os.path.join(LOCAL_TMP_DIR, "orders_clean.csv")


def ingest_from_postgres(**context):
    """Ingest: query Postgres app.orders, ghi CSV raw."""
    os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
    conn = None
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        cursor = conn.cursor()
        query = """
            SELECT order_id, customer_id, order_date, total_amount, status, created_at
            FROM app.orders
            WHERE order_date >= NOW() - INTERVAL '30 days';
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        with open(LOCAL_RAW_PATH, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(colnames)
            writer.writerows(rows)
        logging.info("Ingest: extracted %d rows to %s", len(rows), LOCAL_RAW_PATH)
        context["ti"].xcom_push(key="row_count", value=len(rows))
    except Exception as e:
        logging.exception("Ingest failed")
        raise
    finally:
        if conn:
            conn.close()


def clean_data(**context):
    """Clean: đọc raw CSV, drop row thiếu order_id/customer_id, chuẩn hóa, ghi clean CSV."""
    if not os.path.exists(LOCAL_RAW_PATH):
        raise FileNotFoundError(f"Raw file not found: {LOCAL_RAW_PATH}")
    rows_in = 0
    rows_out = 0
    with open(LOCAL_RAW_PATH, mode="r", newline="", encoding="utf-8") as fin:
        reader = csv.DictReader(fin)
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise ValueError("Empty or invalid CSV")
        with open(LOCAL_CLEAN_PATH, mode="w", newline="", encoding="utf-8") as fout:
            writer = csv.DictWriter(fout, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                rows_in += 1
                if not row.get("order_id") or not row.get("customer_id"):
                    continue
                writer.writerow(row)
                rows_out += 1
    logging.info("Clean: %d rows in, %d rows out -> %s", rows_in, rows_out, LOCAL_CLEAN_PATH)
    context["ti"].xcom_push(key="rows_cleaned", value=rows_out)


def load_to_minio(**context):
    """Load: upload CSV clean lên MinIO."""
    if not os.path.exists(LOCAL_CLEAN_PATH):
        raise FileNotFoundError(f"Clean file not found: {LOCAL_CLEAN_PATH}")
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    try:
        s3.create_bucket(Bucket=MINIO_BUCKET)
    except Exception as e:
        logging.info("Bucket %s may exist: %s", MINIO_BUCKET, str(e))
    s3.upload_file(LOCAL_CLEAN_PATH, MINIO_BUCKET, MINIO_KEY)
    logging.info("Load: uploaded to s3://%s/%s", MINIO_BUCKET, MINIO_KEY)
    context["ti"].xcom_push(key="minio_key", value=MINIO_KEY)


def validate_load(**context):
    """Validate: kiểm tra object tồn tại trên MinIO và có size > 0; (tuỳ chọn) row count."""
    minio_key = context["ti"].xcom_pull(task_ids="load_to_minio", key="minio_key") or MINIO_KEY
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    try:
        head = s3.head_object(Bucket=MINIO_BUCKET, Key=minio_key)
        size = head.get("ContentLength", 0)
        if size <= 0:
            raise ValueError(f"Object s3://{MINIO_BUCKET}/{minio_key} has size {size}")
        logging.info("Validate: s3://%s/%s exists, size=%d", MINIO_BUCKET, minio_key, size)
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            raise FileNotFoundError(f"Object not found: s3://{MINIO_BUCKET}/{minio_key}") from e
        raise


default_args = {
    "owner": "de_bootcamp",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ingest_clean_load_validate",
    default_args=default_args,
    description="ETL: Ingest (Postgres) -> Clean -> Load (MinIO) -> Validate",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["de-bootcamp", "week08", "ingest", "clean", "load", "validate"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_from_postgres",
        python_callable=ingest_from_postgres,
    )

    clean_task = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
    )

    load_task = PythonOperator(
        task_id="load_to_minio",
        python_callable=load_to_minio,
    )

    validate_task = PythonOperator(
        task_id="validate_load",
        python_callable=validate_load,
    )

    ingest_task >> clean_task >> load_task >> validate_task
