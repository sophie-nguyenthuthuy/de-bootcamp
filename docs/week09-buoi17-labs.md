# Week 9 · Buổi 17 — Feature Store & Cloud Integration

**Feature Store lý thuyết & Feast demo · S3, Glue, Athena, EMR mapping · Lab: Deploy Feast + Redis · Lab: Bronze/Silver/Gold → S3 / Query Athena**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Lý thuyết | **Feature Store** — offline/online, Feast |
| Lý thuyết | **S3, Glue, Athena, EMR mapping** — AWS tương đương stack local |
| Lab 1 | **Deploy Feast + Redis** — Redis online store, feature repo, apply & materialize |
| Lab 2 | **Move Bronze/Silver/Gold → S3 / Query Athena** — MinIO = S3, Trino = Athena |

---

## Chuẩn bị

- Docker đang chạy.
- Postgres (app) có dữ liệu `app.orders` nếu Feast dùng làm nguồn.
- MinIO + Trino đã chạy (lakehouse zones từ Buổi 9).

---

## Lý thuyết tóm tắt

### Feature Store — lý thuyết & Feast

- **Feature Store:** Kho lưu features (tính năng) dùng cho training (offline) và serving (online). Đảm bảo **consistency** giữa train và infer, **reuse** features, **versioning**.
- **Offline store:** Lưu feature values theo thời gian (Parquet, BigQuery, Snowflake) — dùng cho training, batch scoring.
- **Online store:** Lưu feature values theo entity key (Redis, DynamoDB) — low-latency serving cho inference.
- **Feast:** Open-source feature store; định nghĩa **Entity**, **Feature View**, **Data Source**; **materialize** từ offline → online; **Python SDK** để retrieve online/historical.

### S3, Glue, Athena, EMR mapping (local ↔ AWS)

| Local (bootcamp) | AWS | Vai trò |
|------------------|-----|---------|
| **MinIO** | **S3** | Object storage (bronze/silver/gold) |
| **Hive Metastore** | **Glue Data Catalog** | Metadata (schema, table, partition) |
| **Trino** | **Athena** | Serverless SQL trên S3 (query engine) |
| **Spark (local / job)** | **EMR** | Cluster Spark xử lý batch/stream |

**Luồng tương đương:** Dữ liệu trên S3 (MinIO) → catalog trong Glue (Hive) → query bằng Athena (Trino). EMR dùng khi cần Spark cluster (ETL, ML).

---

## Lab 1: Deploy Feast + Redis

**Mục tiêu:** Chạy Redis, cấu hình Feast feature repo với Redis làm online store, chạy `feast apply` và (tuỳ chọn) materialize + serve.

### Bước 1 — Khởi động Redis

```bash
cd de-bootcamp
docker compose --profile featurestore up -d redis
```

Kiểm tra: `docker exec -it de_redis redis-cli ping` → `PONG`.

### Bước 2 — Cài Feast (với Redis extra)

```bash
pip install 'feast[redis]'
```

### Bước 3 — Dùng feature repo mẫu

Feature repo mẫu trong **`week09/feast_repo/`**:

- **`feature_store.yaml`:** project, registry (file), provider local, **online_store: type: redis, connection_string: localhost:6379**.
- **`features.py`:** Entity (vd: `customer_id`), Feature View (vd: từ bảng orders hoặc file).

### Bước 4 — Apply và materialize (từ thư mục feature repo)

```bash
cd de-bootcamp/week09/feast_repo
feast apply
```

Áp dụng entity và feature view lên registry và online store.

Để materialize (đổ dữ liệu từ offline → Redis):

```bash
feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S)
```

(Hoặc `feast materialize` với khoảng thời gian nếu có dữ liệu theo timestamp.)

### Bước 5 — (Tuỳ chọn) Lấy feature online bằng Python

```python
from feast import FeatureStore
store = FeatureStore(repo_path="week09/feast_repo")
features = store.get_online_features(
    features=["orders_view:total_amount", "orders_view:order_count"],
    entity_rows=[{"customer_id": 1}],
).to_dict()
```

### Cấu trúc feature repo

```
week09/feast_repo/
  feature_store.yaml   # project, registry, online_store (Redis)
  features.py          # Entity, FeatureView, DataSource
  data/                # (optional) Parquet offline data
```

---

## Lab 2: Move Bronze/Silver/Gold → S3 / Query Athena

**Mục tiêu:** Bronze/Silver/Gold đã nằm trên “S3” (MinIO); query bằng “Athena” (Trino). Trên AWS sẽ dùng S3 + Glue + Athena; local dùng MinIO + Hive Metastore + Trino.

### Bước 1 — Đảm bảo zones trên MinIO (S3)

Đã có từ Buổi 9: schemas `hive.bronze`, `hive.silver`, `hive.gold` với location `s3a://lakehouse/...`. Chạy lại nếu cần:

```bash
# Trong Trino (http://localhost:8080)
```

Nội dung tham khảo: **`sql/week05_buoi9_lakehouse_zones.sql`** (CREATE SCHEMA bronze/silver/gold với location S3).

### Bước 2 — Đẩy dữ liệu mẫu lên Bronze (tuỳ chọn)

Nếu có CSV/Parquet, copy lên MinIO bucket `lakehouse`, prefix `bronze/...` (vd: bằng Airflow DAG hoặc script). Sau đó tạo bảng Hive trỏ tới location đó (CREATE TABLE ... WITH (external_location = 's3a://lakehouse/bronze/...')).

### Bước 3 — Query bằng Trino (Athena tương đương)

Mở Trino UI (http://localhost:8080), chạy:

```sql
SHOW SCHEMAS FROM hive;
USE hive.bronze;
SHOW TABLES;
SELECT * FROM hive.bronze.<table> LIMIT 10;
```

Script mẫu: **`sql/week09_buoi17_s3_athena.sql`** — các lệnh SHOW/CREATE/SELECT cho bronze/silver/gold và query mẫu.

### Bước 4 — Mapping lên AWS (khi deploy cloud)

- **MinIO** → **S3:** bucket `lakehouse` → bucket S3 (vd: `my-data-lake`), prefix `bronze/`, `silver/`, `gold/`.
- **Hive Metastore** → **Glue:** đăng ký schema/table trong Glue Data Catalog (hoặc dùng crawler).
- **Trino** → **Athena:** tạo workgroup Athena, query trực tiếp trên S3 với catalog Glue.

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Redis (featurestore) | `docker compose --profile featurestore up -d redis` — localhost:6379 |
| Feast repo | `week09/feast_repo/` (feature_store.yaml, features.py) |
| Feast apply | `cd week09/feast_repo && feast apply` |
| S3 (local) | MinIO — s3a://lakehouse/bronze/, silver/, gold/ |
| Athena (local) | Trino — http://localhost:8080, catalog hive |
| Zones SQL | `sql/week05_buoi9_lakehouse_zones.sql`, `sql/week09_buoi17_s3_athena.sql` |
