# Week 9 · Buổi 18 — Capstone Project

**End-to-end architecture review · Build final pipeline · Presentation & Feedback**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| 1 | **End-to-end architecture review** — tổng quan stack, luồng dữ liệu |
| 2 | **Build final pipeline** — pipeline hoàn chỉnh từ nguồn → bronze/silver/gold → validate → dashboard |
| 3 | **Presentation & Feedback** — trình bày + nhận xét |

---

## 1. End-to-end architecture review

### Stack tổng quan

| Thành phần | Công nghệ (lab) | Vai trò |
|------------|-----------------|---------|
| **OLTP** | PostgreSQL | Nguồn ứng dụng (app.orders, app.customers, …) |
| **Object storage** | MinIO (S3-compatible) | Bronze / Silver / Gold (lakehouse) |
| **Metadata** | Hive Metastore | Schema, table, partition (Trino/Iceberg) |
| **Query engine** | Trino | SQL trên Hive/Iceberg, S3 |
| **Streaming** | Kafka + Zookeeper | Message bus (CDC, events) |
| **CDC** | Debezium (Kafka Connect) | Postgres WAL → Kafka |
| **Stream processing** | Flink (hoặc Python consumer) | Kafka → MinIO / transform |
| **Orchestration** | Airflow | DAG: ingest → clean → load → validate |
| **BI** | Metabase, Superset | Dashboard, report |
| **Feature store** | Feast + Redis | Online features (serving) |
| **Data quality** | Great Expectations | Validation, expectations |

### Profiles Docker Compose

| Profile | Services | Lệnh |
|---------|----------|------|
| (mặc định) | postgres, minio, hive-metastore, trino, metabase | `docker compose up -d` |
| **streaming** | + zookeeper, kafka, connect, flink-jobmanager, flink-taskmanager, superset | `docker compose --profile streaming up -d` |
| **airflow** | + airflow-db, airflow-init, airflow-webserver, airflow-scheduler | `docker compose --profile airflow up -d` |
| **featurestore** | + redis | `docker compose --profile featurestore up -d` |

### Luồng dữ liệu end-to-end (mô tả)

```
┌─────────────┐     CDC (Debezium)      ┌─────────────┐
│  PostgreSQL │ ──────────────────────► │   Kafka     │
│  (app.*)    │     WAL → topic         │  (dbserver1 │
└─────────────┘                          │   .app.*)   │
       │                                 └──────┬──────┘
       │ batch (Airflow)                        │ stream
       ▼                                        ▼
┌─────────────┐                          ┌─────────────┐
│  Airflow    │                          │  Consumer   │
│  DAG        │                          │  (Python /  │
│  ingest →   │                          │   Flink)    │
│  clean →    │                          └──────┬──────┘
│  load →     │                                 │
│  validate   │                                 │
└──────┬──────┘                                 │
       │                                        │
       ▼                                        ▼
┌─────────────────────────────────────────────────────┐
│              MinIO (S3) — Lakehouse                  │
│  bronze/          silver/          gold/             │
│  (raw, CDC)       (cleaned)        (aggregated)     │
└─────────────────────────────────────────────────────┘
       │
       │ Trino / Athena-like
       ▼
┌─────────────┐     ┌─────────────┐
│  Trino      │     │  Metabase / │
│  (Hive,     │ ──► │  Superset   │
│   Iceberg)  │     │  (Dashboard)│
└─────────────┘     └─────────────┘
```

- **Batch:** Postgres → Airflow DAG (extract CSV → clean → load MinIO) → Bronze/Silver/Gold.
- **Stream:** Postgres → Debezium → Kafka → consumer (Python/Flink) → Bronze (MinIO).
- **Query:** Trino đọc Hive/Iceberg tables trỏ tới S3 (MinIO); BI (Metabase/Superset) kết nối Trino hoặc Postgres.

---

## 2. Build final pipeline

**Mục tiêu:** Chạy một pipeline hoàn chỉnh từ nguồn đến dashboard: **ingest → clean → load → validate → query/dashboard**.

### Bước 1 — Khởi động stack cần thiết

```bash
cd de-bootcamp

# Core (Postgres, MinIO, Hive, Trino, Metabase)
docker compose up -d postgres minio minio-create-bucket hive-metastore-db hive-metastore trino metabase

# (Tuỳ chọn) Streaming: Kafka, Debezium
docker compose --profile streaming up -d zookeeper kafka connect

# (Tuỳ chọn) Airflow
docker compose --profile airflow up -d airflow-db
docker compose --profile airflow run --rm airflow-init
docker compose --profile airflow up -d airflow-webserver airflow-scheduler
```

Đợi các service healthy (Trino, Hive Metastore, Airflow UI).

### Bước 2 — Chuẩn bị dữ liệu nguồn

```bash
# Schema + seed (nếu chưa có)
docker exec -i de_postgres psql -U de_user -d de_db < sql/01_create_oltp_schema.sql
docker exec -i de_postgres psql -U de_user -d de_db < sql/02_seed_sample_data.sql
```

### Bước 3 — Pipeline batch (Airflow)

- Mở **Airflow UI:** http://localhost:8082 (airflow / airflow).
- Bật DAG **ingest_clean_load_validate**, Trigger DAG.
- Kiểm tra từng task: ingest_from_postgres → clean_data → load_to_minio → validate_load.

Kết quả: file CSV trên MinIO `lakehouse/bronze/orders/orders_clean.csv`.

### Bước 4 — (Tuỳ chọn) Pipeline stream (CDC → Bronze)

- Đăng ký Debezium connector (nếu chưa):  
  `curl -X POST -H "Content-Type: application/json" --data @scripts/debezium-postgres-connector.json http://localhost:8083/connectors`
- Chạy consumer CDC → Bronze:  
  `cd week07 && python cdc_to_bronze.py`  
  (chạy nền; INSERT/UPDATE trong Postgres sẽ xuất hiện trên Kafka và ghi file JSON lên MinIO bronze/cdc/orders/).

### Bước 5 — Zones và query (Trino = Athena)

- Tạo schemas Bronze/Silver/Gold (nếu chưa): chạy **`sql/week05_buoi9_lakehouse_zones.sql`** trong Trino (http://localhost:8080).
- Query mẫu: **`sql/week09_buoi17_s3_athena.sql`** (SHOW TABLES, SELECT khi đã có bảng).

### Bước 6 — (Tuỳ chọn) Data quality

```bash
cd week08
pip install -r requirements.txt
python ge_demo.py
```

Validate bảng `app.orders` (Postgres) hoặc CSV mẫu.

### Bước 7 — Dashboard

- **Metabase:** http://localhost:3000 — kết nối Postgres (host=postgres, port=5432, db=de_db), tạo chart/dashboard từ `app.orders` hoặc mart.
- **Superset** (nếu chạy streaming): http://localhost:8088 — kết nối Postgres, dashboard với auto-refresh.

### Checklist pipeline “final”

- [ ] Postgres có schema `app` và dữ liệu mẫu.
- [ ] MinIO có bucket `lakehouse`, zones bronze/silver/gold (prefix).
- [ ] Trino có catalog hive (và optional iceberg), schemas bronze/silver/gold.
- [ ] Airflow DAG **ingest_clean_load_validate** chạy thành công (4 task xanh).
- [ ] (Tuỳ chọn) Debezium connector chạy; consumer CDC → Bronze có file JSON trên MinIO.
- [ ] (Tuỳ chọn) Great Expectations validate pass.
- [ ] Dashboard (Metabase hoặc Superset) có ít nhất một chart từ dữ liệu pipeline.

---

## 3. Presentation & Feedback

### Gợi ý cấu trúc presentation (5–10 phút)

1. **Kiến trúc (1–2 phút)**  
   Trình bày sơ đồ end-to-end: nguồn (Postgres), CDC/stream vs batch, lakehouse (Bronze/Silver/Gold trên S3/MinIO), query engine (Trino), orchestration (Airflow), BI (Metabase/Superset). Nêu rõ công nghệ dùng trong lab (MinIO, Trino, Airflow, …).

2. **Demo pipeline (3–5 phút)**  
   - Trigger Airflow DAG **ingest_clean_load_validate**, cho xem Graph/Logs.  
   - Mở MinIO Console, chỉ file CSV/JSON trên bronze (và silver/gold nếu có).  
   - Chạy 1–2 query Trino trên hive.bronze / hive.silver (hoặc bảng đã tạo).  
   - Mở dashboard (Metabase/Superset), chỉ chart dùng dữ liệu từ pipeline.

3. **Khó khăn & bài học (1–2 phút)**  
   Ví dụ: cấu hình Debezium/Connect, dependency Airflow (Postgres/MinIO), quyền Trino/Iceberg, cách debug DAG hoặc consumer.

4. **Mở rộng (1 phút)**  
   Ý tưởng: thêm Silver/Gold trong Trino/Iceberg, tích hợp GE vào Airflow, đưa pipeline lên AWS (S3, Glue, Athena, EMR).

### Gợi ý tiêu chí feedback (instructor / peer)

| Tiêu chí | Mô tả |
|----------|--------|
| **Kiến trúc** | Rõ ràng, đúng luồng (nguồn → lakehouse → query/dashboard). |
| **Pipeline** | Ingest → clean → load → validate chạy được, có bằng chứng (file MinIO, task success). |
| **Code / config** | DAG, script, SQL nhất quán, dễ chạy lại. |
| **Trình bày** | Thời gian hợp lý, demo thực tế, trả lời câu hỏi. |

### Câu hỏi gợi ý (Q&A)

- Sự khác nhau giữa Bronze, Silver, Gold trong pipeline của bạn?
- Nếu triển khai lên AWS, bạn sẽ map MinIO, Hive, Trino sang dịch vụ nào?
- Bạn sẽ đặt bước validate (GE) ở đâu trong DAG (sau load? sau clean?) và vì sao?

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Trino UI | http://localhost:8080 |
| Airflow UI | http://localhost:8082 (airflow / airflow) |
| Metabase | http://localhost:3000 |
| MinIO Console | http://localhost:9001 |
| DAG final | ingest_clean_load_validate (airflow/dags/) |
| Zones SQL | sql/week05_buoi9_lakehouse_zones.sql, sql/week09_buoi17_s3_athena.sql |
| GE demo | week08/ge_demo.py |
| CDC → Bronze | week07/cdc_to_bronze.py |
