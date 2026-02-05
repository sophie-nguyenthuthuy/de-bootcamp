from datetime import datetime, timedelta
import os
import csv
import logging

import psycopg2
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator

# ============================================================================
# CONFIG
# ============================================================================

POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "de_db"
POSTGRES_USER = "de_user"
POSTGRES_PASSWORD = "de_pass"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "lakehouse"
MINIO_KEY = "bronze/orders/orders_extract.csv"

LOCAL_TMP_DIR = "/tmp/de_etl"
LOCAL_CSV_PATH = os.path.join(LOCAL_TMP_DIR, "orders_extract.csv")


def extract_from_postgres(**context):
    """
    Query dữ liệu từ PostgreSQL và ghi ra file CSV tại /tmp/de_etl/orders_extract.csv
    """
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
            SELECT
                o.id AS order_id,
                o.customer_id,
                o.order_date,
                o.total_amount
            FROM app.orders o
            WHERE o.order_date >= NOW() - INTERVAL '30 days';
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]

        with open(LOCAL_CSV_PATH, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(colnames)
            writer.writerows(rows)

        logging.info("Extracted %d rows from Postgres to %s", len(rows), LOCAL_CSV_PATH)

    except Exception as e:
        logging.exception("Error extracting data from Postgres")
        raise e
    finally:
        if conn:
            conn.close()


def transform_data(**context):
    """
    Transform đơn giản.
    Ở đây chỉ log – có thể mở rộng để làm sạch dữ liệu.
    """
    if not os.path.exists(LOCAL_CSV_PATH):
        raise FileNotFoundError(f"{LOCAL_CSV_PATH} not found")

    size = os.path.getsize(LOCAL_CSV_PATH)
    logging.info("Transform step - current CSV size: %d bytes", size)


def load_to_minio(**context):
    """
    Upload file CSV lên MinIO (S3-compatible)
    """
    if not os.path.exists(LOCAL_CSV_PATH):
        raise FileNotFoundError(f"{LOCAL_CSV_PATH} not found")

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
        logging.info("Bucket %s có thể đã tồn tại: %s", MINIO_BUCKET, str(e))

    s3.upload_file(LOCAL_CSV_PATH, MINIO_BUCKET, MINIO_KEY)
    logging.info("Uploaded %s to s3://%s/%s", LOCAL_CSV_PATH, MINIO_BUCKET, MINIO_KEY)


default_args = {
    "owner": "de_bootcamp",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="postgres_to_minio_etl",
    default_args=default_args,
    description="ETL: Extract from Postgres -> CSV -> MinIO (bronze)",
    schedule_interval=None,  # hoặc '@daily'
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["de-bootcamp", "postgres", "minio"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_from_postgres",
        python_callable=extract_from_postgres,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_to_minio",
        python_callable=load_to_minio,
    )

    extract_task >> transform_task >> load_task
