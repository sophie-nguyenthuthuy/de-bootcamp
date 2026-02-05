"""
Week 5 · Buổi 10 — Spark batch: raw (CSV) → clean → silver (Parquet)
DataFrame API, Catalyst, Tungsten.
Usage:
  Local:  python spark_bronze_to_silver.py <input_csv> <output_parquet_dir>
  S3:     spark-submit --packages hadoop-aws ... spark_bronze_to_silver.py s3a://... s3a://...
"""
from __future__ import annotations

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def get_spark(endpoint: str | None = None, access_key: str | None = None, secret_key: str | None = None):
    """Build SparkSession; optionally configure S3/MinIO."""
    builder = SparkSession.builder.appName("bronze_to_silver")
    if endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
        if access_key and secret_key:
            builder = (
                builder.config("spark.hadoop.fs.s3a.access.key", access_key)
                .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            )
    return builder.getOrCreate()


def clean_df(spark, df):
    """Clean: drop nulls in key cols, trim strings, cast numeric, drop duplicates."""
    # Trim string columns
    for c in df.columns:
        if df.schema[c].dataType.simpleString() == "string":
            df = df.withColumn(c, F.trim(F.col(c)))
    # Drop rows with null in first column (e.g. product_name or order_id)
    first_col = df.columns[0]
    df = df.dropna(subset=[first_col])
    # Cast price/total_amount to double if present
    for col_name in ("price", "total_amount", "subtotal"):
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
    # Drop duplicates on all columns
    df = df.dropDuplicates()
    return df


def main():
    if len(sys.argv) < 3:
        print("Usage: spark_bronze_to_silver.py <input_csv_path> <output_parquet_path> [s3_endpoint] [access_key] [secret_key]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    endpoint = sys.argv[3] if len(sys.argv) > 3 else None
    access_key = sys.argv[4] if len(sys.argv) > 4 else os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = sys.argv[5] if len(sys.argv) > 5 else os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    spark = get_spark(endpoint=endpoint, access_key=access_key, secret_key=secret_key)

    # Read CSV (header, inferSchema for local/small files)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    print(f"Read {df.count()} rows from {input_path}")

    df_clean = clean_df(spark, df)
    n_clean = df_clean.count()
    print(f"After clean: {n_clean} rows")

    df_clean.write.mode("overwrite").parquet(output_path)
    print(f"Wrote Parquet to {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
