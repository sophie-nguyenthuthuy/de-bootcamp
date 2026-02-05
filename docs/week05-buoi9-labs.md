# Week 5 · Buổi 9 — Data Lakehouse Architecture

**Object Storage · Delta Lake vs Iceberg vs Hudi · Trino vs Presto vs Hive · Deploy MinIO + Trino + Hive Metastore · Tạo 3 zones bronze / silver / gold**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Lý thuyết | **Object Storage** — S3/MinIO, bucket, path, s3a |
| Lý thuyết | **Delta Lake vs Iceberg vs Hudi** — table format, ACID, time travel |
| Lý thuyết | **Trino vs Presto vs Hive** — query engine, metastore, catalog |
| Lab 1 | **Deploy MinIO + Trino + Hive Metastore** — chạy stack, kiểm tra kết nối |
| Lab 2 | **Tạo 3 zones: bronze / silver / gold** — Hive schemas trên S3 (MinIO) |

---

## Chuẩn bị

- Docker đang chạy.
- Repo đã clone; thư mục `trino/etc` có đủ config (Buổi trước đã cấu hình Trino + Hive + MinIO).

---

## Lý thuyết tóm tắt

### Object Storage

- **S3 / MinIO:** Object storage tương thích S3; lưu file theo bucket + key (path).
- **Bucket:** Container logic (vd: `lakehouse`, `raw`, `bronze`, `silver`, `gold`).
- **s3a:** Protocol Hadoop/Spark/Trino dùng để đọc ghi S3 (vd: `s3a://lakehouse/bronze/`).

### Delta Lake vs Iceberg vs Hudi

| | Delta Lake | Apache Iceberg | Apache Hudi |
|---|-------------|----------------|-------------|
| **Nguồn** | Databricks | Netflix / Apache | Uber / Apache |
| **ACID** | Có | Có | Có |
| **Time travel** | Có | Có | Có |
| **Engine** | Spark, Trino (connector) | Spark, Trino, Flink | Spark, Flink |
| **File** | Parquet + log | Metadata + data files | Parquet + log |

Trong lab dùng Hive connector (metadata trong Hive Metastore, data trên S3); Iceberg/Delta có connector riêng trên Trino nếu cần sau.

### Trino vs Presto vs Hive

| | Trino | Presto | Hive |
|---|-------|--------|------|
| **Vai trò** | Query engine (SQL) | Query engine (fork cũ) | Query engine + metastore |
| **Metastore** | Dùng Hive Metastore (hoặc Iceberg catalog) | Tương tự | Có sẵn Hive Metastore |
| **Catalog** | Connector: hive, iceberg, delta | Connector | Catalog = metastore |

**Trong stack:** Trino = query engine; Hive Metastore = metadata (bảng, schema, location); MinIO = object storage (data).

---

## Lab 1: Deploy MinIO + Trino + Hive Metastore

**Mục tiêu:** Chạy stack lakehouse (MinIO, Hive Metastore DB, Hive Metastore, Trino) và kiểm tra kết nối.

### Bước 1 — Khởi động stack

```bash
cd de-bootcamp
docker compose up -d minio minio-create-bucket hive-metastore-db hive-metastore trino
```

Đợi Hive Metastore healthy (30–60 giây), Trino khởi động (30–60 giây).

### Bước 2 — Kiểm tra MinIO

- **Console:** http://localhost:9001 — đăng nhập `minioadmin` / `minioadmin`.
- **Bucket:** Cần thấy `lakehouse`, `raw`, `bronze`, `silver`, `gold` (tạo bởi `minio-create-bucket`).
- **API:** `curl -s http://localhost:9000/minio/health/live` → 200 OK.

### Bước 3 — Kiểm tra Hive Metastore

```bash
docker compose ps
# hive-metastore phải Up (hoặc healthy)
nc -zv localhost 9083   # hoặc: curl -s http://localhost:9083 (nếu có HTTP)
```

### Bước 4 — Kiểm tra Trino

- **UI:** http://localhost:8080
- **API:** `curl -s http://localhost:8080/v1/info` → JSON (nodeVersion, coordinator, …)
- **SQL (Trino CLI hoặc UI):**

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM hive;
```

Cần thấy catalog `hive` và (sau Lab 2) các schema `bronze`, `silver`, `gold`.

---

## Lab 2: Tạo 3 zones — bronze / silver / gold

**Mục tiêu:** Tạo Hive schemas trỏ tới S3 (MinIO) cho 3 tầng: bronze (raw/landing), silver (cleaned), gold (aggregated).

### Mô hình zones

| Zone | Mục đích | Location S3 (MinIO) |
|------|----------|----------------------|
| **bronze** | Dữ liệu thô / landing (CSV, JSON, …) | `s3a://lakehouse/bronze/` |
| **silver** | Dữ liệu đã clean, chuẩn hóa (vd: Parquet) | `s3a://lakehouse/silver/` |
| **gold** | Dữ liệu tổng hợp cho analytics / BI | `s3a://lakehouse/gold/` |

### Bước 1 — Tạo schemas trong Trino

Chạy trong Trino UI (http://localhost:8080) hoặc Trino CLI:

```sql
-- Bronze: raw / landing
CREATE SCHEMA IF NOT EXISTS hive.bronze
WITH (location = 's3a://lakehouse/bronze/');

-- Silver: cleaned
CREATE SCHEMA IF NOT EXISTS hive.silver
WITH (location = 's3a://lakehouse/silver/');

-- Gold: aggregated / analytics
CREATE SCHEMA IF NOT EXISTS hive.gold
WITH (location = 's3a://lakehouse/gold/');

SHOW SCHEMAS FROM hive;
```

### Bước 2 — (Tuỳ chọn) Tạo bảng mẫu trong từng zone

Ví dụ bảng external đọc CSV từ bronze (sau khi đã copy file lên MinIO):

```sql
-- Ví dụ: bảng bronze đọc CSV (khi đã có file s3a://lakehouse/bronze/orders/orders.csv)
CREATE TABLE IF NOT EXISTS hive.bronze.orders (
    order_id      integer,
    customer_id   integer,
    order_date    date,
    total_amount  double
)
WITH (
    format = 'CSV',
    skip_header_line_count = 1,
    external_location = 's3a://lakehouse/bronze/orders/'
);
```

Silver/Gold thường dùng Parquet; tạo bảng khi đã có pipeline ghi (vd: Spark, Airflow DAG).

### Bước 3 — Kiểm tra trong MinIO

- Vào MinIO Console → bucket `lakehouse` → xem thư mục `bronze/`, `silver/`, `gold/`.
- Schemas Trino chỉ định nghĩa metadata; thư mục S3 có thể trống cho đến khi có job ghi data.

### File SQL mẫu (chạy trong Trino)

**`sql/week05_buoi9_lakehouse_zones.sql`** — chứa lệnh CREATE SCHEMA cho bronze, silver, gold (copy vào Trino UI hoặc dùng Trino CLI).

---

## Demo flow gợi ý (instructor)

1. **Lý thuyết (25–30 phút):** Object Storage (S3/MinIO, s3a), Delta vs Iceberg vs Hudi (bảng so sánh), Trino vs Presto vs Hive (engine + metastore).
2. **Lab 1 — Deploy (20–25 phút):** `docker compose up -d` minio, hive-metastore-db, hive-metastore, trino; kiểm tra MinIO Console, Trino UI, `SHOW CATALOGS` / `SHOW SCHEMAS FROM hive`.
3. **Lab 2 — Zones (20–25 phút):** Chạy CREATE SCHEMA bronze/silver/gold trong Trino; kiểm tra MinIO bucket structure; (tuỳ chọn) tạo 1 bảng mẫu external.
4. **Kết (5 phút):** Tóm tắt: lakehouse = object storage + metadata (Hive) + query engine (Trino); 3 zones = bronze (raw) → silver (clean) → gold (aggregated).

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| MinIO Console | http://localhost:9001 (minioadmin / minioadmin) |
| MinIO S3 API | http://localhost:9000 |
| Trino UI | http://localhost:8080 |
| Hive Metastore | localhost:9083 (Thrift) |
| Buckets | lakehouse, raw, bronze, silver, gold |
| S3 path (Trino) | s3a://lakehouse/bronze/, s3a://lakehouse/silver/, s3a://lakehouse/gold/ |
| Lệnh deploy | `docker compose up -d minio minio-create-bucket hive-metastore-db hive-metastore trino` |
| File zones SQL | `sql/week05_buoi9_lakehouse_zones.sql` |
