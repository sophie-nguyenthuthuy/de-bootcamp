# Week 8 · Buổi 15 — Airflow Orchestration

**DAG · Operators · Sensors · Airflow vs Cron · Lab: Deploy Airflow với Docker · Lab: DAG ingest → clean → load → validate**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Lý thuyết | **DAG, Operators, Sensors** — định nghĩa, vai trò |
| Lý thuyết | **Airflow vs Cron** — lợi thế orchestration |
| Lab 1 | **Deploy Airflow với Docker** — profile airflow, init, webserver, scheduler |
| Lab 2 | **Build DAG: ingest → clean → load → validate** — pipeline 4 bước với PythonOperator |

---

## Chuẩn bị

- Docker đang chạy.
- Repo có thư mục `airflow/dags/` và `docker-compose.yml` với profile `airflow`.
- Postgres (app) và MinIO đang chạy nếu DAG cần extract/load (có thể chạy core stack trước).

---

## Lý thuyết tóm tắt

### DAG, Operators, Sensors

- **DAG (Directed Acyclic Graph):** Đồ thị có hướng, không chu trình; mỗi node = task, cạnh = dependency. Định nghĩa **workflow** (thứ tự thực thi, retry, schedule).
- **Operator:** Đơn vị công việc (task) — thực thi một hành động (chạy script, gọi API, query DB). Ví dụ: `PythonOperator`, `BashOperator`, `PostgresOperator`, `SparkSubmitOperator`.
- **Sensor:** Operator đợi điều kiện thỏa (file xuất hiện, partition Hive có data, Kafka topic có message). **Poke** định kỳ cho đến khi success hoặc timeout.

### Airflow vs Cron

| | Cron | Airflow |
|---|------|---------|
| **Mô hình** | Job độc lập, chạy theo lịch | DAG nhiều task, dependency rõ ràng |
| **Retry / alert** | Tự xử lý (script, mail) | Retry, SLA, alert tích hợp |
| **Visibility** | Log file, script | UI: graph, logs, history |
| **Backfill / catchup** | Thủ công | Backfill theo DAG run, catchup |
| **Scale** | Một máy / crontab | Scheduler + executor (Local/Celery/K8s) |

**Khi dùng Airflow:** Pipeline nhiều bước (ingest → clean → load → validate), cần retry, monitoring, backfill. **Khi dùng Cron:** Job đơn giản, chạy định kỳ một lệnh.

---

## Lab 1: Deploy Airflow với Docker

**Mục tiêu:** Chạy Airflow (Postgres metadata, webserver, scheduler) bằng Docker Compose profile `airflow`, truy cập UI và xem DAG.

### Bước 1 — Khởi động Airflow DB và init (lần đầu)

```bash
cd de-bootcamp
docker compose --profile airflow up -d airflow-db
```

Đợi Postgres healthy (vài giây). Sau đó chạy init **một lần** (tạo schema, user admin):

```bash
docker compose --profile airflow run --rm airflow-init
```

Kết quả mong đợi: `Airflow DB initialized.` (hoặc thông báo admin user đã tạo).

### Bước 2 — Khởi động Webserver và Scheduler

```bash
docker compose --profile airflow up -d airflow-webserver airflow-scheduler
```

Đợi 30–60 giây cho webserver sẵn sàng.

### Bước 3 — Truy cập Airflow UI

- **URL:** http://localhost:8082  
- **Login:** `airflow` / `airflow`

(Trino dùng port 8080 nên Airflow webserver map ra **8082**.)

### Bước 4 — Kiểm tra DAG

- **DAGs:** Trong UI sẽ thấy các DAG trong `airflow/dags/` (vd: `postgres_to_minio_etl`, `ingest_clean_load_validate`).
- Bật DAG (toggle), Trigger DAG để chạy thử.

### Bước 5 — (Tuỳ chọn) Chạy lệnh CLI trong container

```bash
docker exec -it de_airflow_webserver airflow dags list
docker exec -it de_airflow_webserver airflow dags trigger ingest_clean_load_validate
```

---

## Lab 2: Build DAG ingest → clean → load → validate

**Mục tiêu:** Tạo DAG 4 task: **ingest** (extract từ Postgres) → **clean** (transform, chuẩn hóa) → **load** (ghi lên MinIO) → **validate** (kiểm tra file/row count).

### Cấu trúc DAG

```
ingest_from_postgres → clean_data → load_to_minio → validate_load
```

- **ingest:** Query `app.orders` (hoặc bảng phù hợp), ghi CSV tạm.
- **clean:** Đọc CSV, drop null/duplicate, chuẩn hóa kiểu, ghi lại CSV sạch.
- **load:** Upload CSV lên MinIO `lakehouse/bronze/orders/` (hoặc path trong DAG).
- **validate:** Kiểm tra object tồn tại trên MinIO và (tuỳ chọn) row count / schema cơ bản.

### File DAG

DAG mẫu: **`airflow/dags/ingest_clean_load_validate.py`**.

- Dùng **PythonOperator** cho cả 4 task.
- Dùng **XCom** (hoặc biến chung trong cùng process) để truyền đường dẫn file giữa ingest → clean → load; validate nhận path/key MinIO từ load.
- **default_args:** retries=1, retry_delay=5 phút, không email.
- **Schedule:** `@daily` hoặc `None` (chỉ trigger tay).

### Sau khi thêm DAG

1. Lưu file vào `airflow/dags/`.
2. Đợi scheduler scan (30–60 giây) hoặc refresh UI.
3. Bật DAG **ingest_clean_load_validate**, Trigger DAG.
4. Xem Graph / Grid / Logs từng task.

### Kiểm tra kết quả

- **MinIO:** Bucket `lakehouse`, prefix `bronze/orders/` (hoặc path trong DAG) — có file CSV sau load.
- **Validate task:** Success nếu check (file tồn tại / row count) pass.

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Airflow UI | http://localhost:8082 |
| Login | airflow / airflow |
| Profile | `docker compose --profile airflow up -d` |
| Init (lần đầu) | `docker compose --profile airflow run --rm airflow-init` |
| DAG folder | `./airflow/dags` (mount vào `/opt/airflow/dags`) |
| DAG mẫu (4 bước) | `airflow/dags/ingest_clean_load_validate.py` |
| Postgres (app) | host=postgres, port=5432, db=de_db (cho extract) |
| MinIO | endpoint http://minio:9000, bucket lakehouse (cho load) |
