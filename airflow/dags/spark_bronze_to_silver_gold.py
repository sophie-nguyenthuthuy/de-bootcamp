from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_APP_PATH = "/opt/airflow/spark_jobs/bronze_to_silver_gold.py"

BRONZE_PATH = "s3a://lakehouse/bronze/orders/orders_extract.csv"
SILVER_PATH = "s3a://lakehouse/silver/orders_clean/"
GOLD_PATH = "s3a://lakehouse/gold/orders_daily_metrics/"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

default_args = {
    "owner": "de_bootcamp",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_bronze_to_silver_gold",
    default_args=default_args,
    description="Spark job: Bronze CSV in MinIO -> Silver (clean) -> Gold (daily metrics)",
    schedule_interval=None,  # hoặc '@daily'
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["de-bootcamp", "spark", "minio", "lakehouse"],
) as dag:

    spark_task = SparkSubmitOperator(
        task_id="bronze_to_silver_gold",
        application=SPARK_APP_PATH,
        conn_id="spark_default",  # config trong Airflow UI
        application_args=[
            BRONZE_PATH,
            SILVER_PATH,
            GOLD_PATH,
            MINIO_ENDPOINT,
            MINIO_ACCESS_KEY,
            MINIO_SECRET_KEY,
        ],
        conf={
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        },
    )
