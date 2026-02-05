"""
Week 5 · Buổi 10 — Benchmark Parquet vs CSV
So sánh thời gian đọc/ghi và dung lượng (cùng dataset).
Usage: python benchmark_parquet_vs_csv.py [output_dir]
        spark-submit benchmark_parquet_vs_csv.py
"""
from __future__ import annotations

import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def dir_size_mb(path: str) -> float:
    """Approximate directory size in MB (sum of file sizes)."""
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total / (1024 * 1024)


def main():
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "./benchmark_output"
    os.makedirs(out_dir, exist_ok=True)
    csv_path = os.path.join(out_dir, "data_csv")
    parquet_path = os.path.join(out_dir, "data_parquet")

    spark = SparkSession.builder.appName("benchmark_parquet_vs_csv").getOrCreate()

    # Generate sample DataFrame (~100k rows)
    n = 100_000
    df = spark.range(n).select(
        F.col("id"),
        F.lit("product_" + F.col("id").cast("string")).alias("name"),
        (F.rand() * 10).cast("int").alias("category_id"),
        (F.rand() * 1_000_000 + 10_000).alias("price"),
    )
    df.cache()
    df.count()

    results = []

    # ---- Write CSV ----
    t0 = time.perf_counter()
    df.write.mode("overwrite").option("header", "true").csv(csv_path)
    write_csv_time = time.perf_counter() - t0
    size_csv = dir_size_mb(csv_path) if os.path.isdir(csv_path) else 0
    results.append(("CSV", "write", write_csv_time, size_csv))

    # ---- Write Parquet ----
    t0 = time.perf_counter()
    df.write.mode("overwrite").parquet(parquet_path)
    write_parquet_time = time.perf_counter() - t0
    size_parquet = dir_size_mb(parquet_path) if os.path.isdir(parquet_path) else 0
    results.append(("Parquet", "write", write_parquet_time, size_parquet))

    # ---- Read CSV ----
    t0 = time.perf_counter()
    spark.read.option("header", "true").csv(csv_path).count()
    read_csv_time = time.perf_counter() - t0
    results.append(("CSV", "read", read_csv_time, None))

    # ---- Read Parquet ----
    t0 = time.perf_counter()
    spark.read.parquet(parquet_path).count()
    read_parquet_time = time.perf_counter() - t0
    results.append(("Parquet", "read", read_parquet_time, None))

    spark.stop()

    # Report
    print("\n=== Benchmark Parquet vs CSV (same dataset, ~100k rows) ===\n")
    print(f"{'Format':<10} {'Op':<6} {'Time (s)':<12} {'Size (MB)':<10}")
    print("-" * 42)
    for fmt, op, t, size in results:
        size_str = f"{size:.2f}" if size is not None else "-"
        print(f"{fmt:<10} {op:<6} {t:<12.3f} {size_str:<10}")
    print("-" * 42)
    print(f"\nParquet write speedup: {write_csv_time / write_parquet_time:.2f}x")
    print(f"Parquet read speedup:  {read_csv_time / read_parquet_time:.2f}x")
    print(f"Parquet size ratio:   {size_parquet / size_csv:.2%} of CSV")
    print(f"\nOutput dir: {out_dir}")


if __name__ == "__main__":
    main()
