# Week 5 · Buổi 10 — Spark Batch Processing

**RDD → DataFrame API · Catalyst optimizer · Tungsten (đọc/ghi Lakehouse) · Spark batch raw → clean → silver · Benchmark Parquet vs CSV**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Lý thuyết | **Spark RDD → DataFrame API** — transformation, action, lazy evaluation |
| Lý thuyết | **Catalyst optimizer** — logical plan, physical plan, rule-based optimization |
| Lý thuyết | **Tungsten** — off-heap, columnar, codegen (đọc/ghi Lakehouse nhanh hơn) |
| Lab 1 | **Spark batch job: raw → clean → silver** — đọc CSV, clean (DataFrame), ghi Parquet |
| Lab 2 | **Benchmark Parquet vs CSV** — so sánh thời gian đọc/ghi và dung lượng |

---

## Chuẩn bị

- Python 3.10+ với **PySpark** (`pip install pyspark`).
- (Tuỳ chọn) MinIO + Spark trong Docker — nếu chạy job đọc/ghi S3 (s3a).
- File lab: **`week05/spark_bronze_to_silver.py`**, **`week05/benchmark_parquet_vs_csv.py`**.

---

## Lý thuyết tóm tắt

### RDD → DataFrame API

- **RDD:** Resilient Distributed Dataset — API cấp thấp, transformation (map, filter) + action (collect, count).
- **DataFrame:** API cấp cao, schema, Catalyst tối ưu; built on RDD.
- **Lazy evaluation:** Transformation không chạy ngay; chỉ khi gặp action mới thực thi (tối ưu được cả DAG).

### Catalyst optimizer

- **Logical plan:** Query → logical plan (tree).
- **Rules:** predicate pushdown, constant folding, column pruning, …
- **Physical plan:** Chọn strategy (Broadcast join, Sort merge join, …).
- **Kết quả:** Ít shuffle, ít I/O, query nhanh hơn so với RDD thuần.

### Tungsten

- **Off-heap memory:** Giảm GC, quản lý bộ nhớ hiệu quả.
- **Columnar format (trong memory):** Cache/process theo cột → phù hợp analytics.
- **Code generation:** Sinh bytecode cho expression/operator → giảm overhead.
- **Đọc/ghi Lakehouse:** Parquet/ORC được tối ưu (columnar, predicate pushdown) → đọc/ghi nhanh hơn CSV.

---

## Lab 1: Spark batch job — raw → clean → silver

**Mục tiêu:** Viết job PySpark: đọc CSV (raw/bronze), clean bằng DataFrame API, ghi Parquet (silver).

### Cấu trúc job (`week05/spark_bronze_to_silver.py`)

1. **Đọc:** `spark.read.option("header", True).csv(bronze_path)` — CSV có header.
2. **Clean:** Drop null, trim string, cast type (vd: `total_amount` → double); drop duplicate (optional).
3. **Ghi:** `df.write.mode("overwrite").parquet(silver_path)` — Parquet partition (optional: theo `order_date`).

### Chạy local (không cần MinIO)

```bash
cd week05
pip install pyspark

# Dùng file local: đọc CSV mẫu, ghi Parquet ra thư mục local
python spark_bronze_to_silver.py \
  ../week03/sample_data/products.csv \
  ./output/silver/products
```

### Chạy với MinIO (s3a)

Cần cấu hình Spark: `spark.hadoop.fs.s3a.endpoint`, `spark.hadoop.fs.s3a.path.style.access`, credentials. Ví dụ (trong script hoặc spark-submit):

```bash
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4 \
  week05/spark_bronze_to_silver.py \
  s3a://lakehouse/bronze/orders/orders_extract.csv \
  s3a://lakehouse/silver/orders_clean/
```

### Kiểm tra

- Thư mục silver có file Parquet (part-*.parquet) hoặc trong MinIO bucket `lakehouse/silver/...`.
- Đọc lại bằng Trino hoặc `spark.read.parquet(silver_path)`.

---

## Lab 2: Benchmark Parquet vs CSV

**Mục tiêu:** So sánh thời gian đọc/ghi và dung lượng khi dùng CSV và Parquet (cùng dataset).

### Script (`week05/benchmark_parquet_vs_csv.py`)

1. **Tạo DataFrame mẫu** (vd: 100k–1M dòng) hoặc đọc từ một nguồn có sẵn.
2. **Ghi CSV:** `df.write.mode("overwrite").csv(csv_path)` — đo thời gian và dung lượng thư mục.
3. **Ghi Parquet:** `df.write.mode("overwrite").parquet(parquet_path)` — đo thời gian và dung lượng.
4. **Đọc CSV:** `spark.read.csv(csv_path)` — đo thời gian (vd: count hoặc collect).
5. **Đọc Parquet:** `spark.read.parquet(parquet_path)` — đo thời gian.
6. **In kết quả:** Bảng hoặc log: format, write time, read time, size (MB).

### Chạy

```bash
cd week05
python benchmark_parquet_vs_csv.py
# Hoặc: spark-submit benchmark_parquet_vs_csv.py
```

### Kỳ vọng

- **Parquet:** Ghi/đọc thường nhanh hơn CSV; dung lượng nhỏ hơn (nén, columnar).
- **CSV:** Dễ đọc bằng text tool; không nén, I/O nhiều hơn.

---

## Demo flow gợi ý (instructor)

1. **Lý thuyết (25–30 phút):** RDD vs DataFrame, lazy eval; Catalyst (logical → physical); Tungsten (off-heap, columnar, codegen).
2. **Lab 1 — Spark batch (35–40 phút):** Mở `spark_bronze_to_silver.py`, giải thích read → clean (DataFrame) → write Parquet; chạy local với CSV mẫu; (tuỳ chọn) chạy với s3a nếu có MinIO.
3. **Lab 2 — Benchmark (20–25 phút):** Chạy `benchmark_parquet_vs_csv.py`; xem bảng kết quả; giải thích tại sao Parquet thường tốt hơn cho lakehouse.
4. **Kết (5 phút):** Tóm tắt: DataFrame API + Catalyst + Tungsten → batch Lakehouse; Parquet = format chuẩn cho silver/gold.

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Spark job (raw → silver) | `week05/spark_bronze_to_silver.py` |
| Benchmark script | `week05/benchmark_parquet_vs_csv.py` |
| CSV mẫu (local) | `week03/sample_data/products.csv` |
| S3 bronze (MinIO) | `s3a://lakehouse/bronze/orders/` |
| S3 silver (MinIO) | `s3a://lakehouse/silver/orders_clean/` |
| Cài PySpark | `pip install pyspark` |
