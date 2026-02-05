# Week 7 · Buổi 14 — Streaming (Flink / Spark Structured Streaming)

**Stateless vs Stateful · Time semantics, watermark · Lab: Kafka → Flink → MinIO · Lab: Realtime dashboard với Superset**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Lý thuyết | **Stateless vs Stateful** — xử lý không lưu trạng thái vs có state |
| Lý thuyết | **Time semantics, watermark** — event time, processing time, late data |
| Lab 1 | **Pipeline real-time Kafka → Flink → MinIO** — Flink SQL hoặc Python fallback |
| Lab 2 | **Realtime dashboard bằng Superset** — kết nối Postgres, dashboard theo thời gian |

---

## Chuẩn bị

- Docker đang chạy.
- Stack streaming: `docker compose --profile streaming up -d` (Kafka, Connect, Flink, Superset).
- Topic Kafka có dữ liệu (vd: `lab-events` từ Buổi 12, hoặc `dbserver1.app.orders` từ CDC).
- MinIO đang chạy (bucket `lakehouse`).

---

## Lý thuyết tóm tắt

### Stateless vs Stateful

- **Stateless:** Mỗi record xử lý độc lập, không lưu thông tin giữa các record (vd: map, filter). Dễ scale, không cần checkpoint state.
- **Stateful:** Cần nhớ thông tin qua nhiều record (vd: window, join, aggregate). Flink/Spark lưu state (trên disk/checkpoint); cho phép window, session, exactly-once.

### Time semantics, watermark

- **Event time:** Thời gian xảy ra sự kiện (trường timestamp trong data). Cần cho window theo “thời gian thật”.
- **Processing time:** Thời gian máy xử lý. Đơn giản, không cần watermark.
- **Watermark:** Cơ chế đánh dấu “đã nhận đủ event đến thời điểm T” → đóng window và emit kết quả. Cho phép xử lý late data (allowed lateness).

### Spark Structured Streaming vs Flink

- **Spark:** Micro-batch hoặc continuous; API DataFrame; checkpoint cho state.
- **Flink:** Native streaming; event time + watermark mạnh; state backend, savepoint. Trong lab dùng Flink (SQL hoặc DataStream) hoặc pipeline Python tương đương.

---

## Lab 1: Build real-time streaming pipeline Kafka → Flink → MinIO

**Mục tiêu:** Đọc stream từ Kafka, (tùy chọn) xử lý/transform, ghi ra MinIO (S3). Có hai cách: **Flink SQL** (cần thêm Kafka connector JAR) hoặc **Python** (chạy ngay, không cần Flink JAR).

### Cách A — Pipeline bằng Python (chạy ngay)

Script `week07/stream_kafka_to_minio.py` đọc topic Kafka (vd: `lab-events`), transform nhẹ (vd: thêm `processed_at`), ghi từng message lên MinIO `lakehouse/bronze/stream/`. Tương tự luồng Kafka → process → MinIO.

**Bước 1 — Cài dependency**

```bash
cd de-bootcamp/week07
pip install -r requirements.txt
```

**Bước 2 — Tạo topic và gửi vài message (nếu chưa có)**

```bash
docker exec -it de_kafka kafka-topics --create --bootstrap-server localhost:9092 --topic lab-events --partitions 2 --replication-factor 1 || true
docker exec -it de_kafka kafka-console-producer --bootstrap-server localhost:9092 --topic lab-events
# Gõ vài dòng, Ctrl+D
```

**Bước 3 — Chạy pipeline**

```bash
python stream_kafka_to_minio.py
```

Script chạy liên tục: consume từ `lab-events`, ghi JSON lên MinIO `lakehouse/bronze/stream/`. Kiểm tra MinIO Console (bucket `lakehouse`, prefix `bronze/stream/`).

### Cách B — Flink SQL (Kafka → MinIO)

Cần thêm **Kafka connector JAR** vào Flink (xem `scripts/download_flink_connectors.sh`). Sau khi có JAR:

**Bước 1 — Khởi động Flink**

```bash
docker compose --profile streaming up -d flink-jobmanager flink-taskmanager
```

**Bước 2 — Copy JAR Kafka connector vào Flink lib**

```bash
./scripts/download_flink_connectors.sh
# Hoặc copy thủ công: flink-sql-connector-kafka-*.jar vào volume/custom lib và mount vào /opt/flink/lib
```

**Bước 3 — Chạy Flink SQL**

Mở Flink Web UI: http://localhost:8081. Submit job qua SQL Gateway hoặc CLI. Nội dung tham khảo trong `week07/flink_kafka_to_minio.sql`:

- `CREATE TABLE` nguồn Kafka (topic `lab-events`, bootstrap `kafka:9092`).
- `CREATE TABLE` đích filesystem (path `s3://lakehouse/bronze/flink/`, cấu hình endpoint MinIO nếu cần).
- `INSERT INTO sink SELECT * FROM source`.

(Lưu ý: Ghi ra S3/MinIO từ Flink SQL cần cấu hình endpoint và credentials; xem comment trong file SQL.)

---

## Lab 2: Realtime dashboard bằng Superset

**Mục tiêu:** Chạy Superset, kết nối Postgres (bảng `app.orders` hoặc view theo thời gian), tạo dashboard với refresh ngắn để cảm nhận “realtime”.

### Bước 1 — Khởi động Superset

```bash
docker compose --profile streaming up -d superset
```

Đợi vài phút (db upgrade, init). Truy cập: **http://localhost:8088**.

### Bước 2 — Đăng nhập

- Username: **admin**
- Password: **admin** (đổi sau lần đầu đăng nhập nếu cần)

### Bước 3 — Thêm database Postgres

- **Settings → Database Connections → + Database.**
- **Supported databases:** PostgreSQL.
- **Display name:** `Postgres App`.
- **SQLAlchemy URI:**  
  `postgresql://de_user:de_pass@postgres:5432/de_db`  
  (trong Docker network hostname là `postgres`; nếu Superset chạy ngoài Docker dùng `localhost:5432`.)
- **Test Connection** → **Save**.

### Bước 4 — Tạo dataset và chart

- **Datasets → + Dataset** → chọn database `Postgres App`, schema `app`, table `orders` (hoặc view có cột thời gian).
- **Charts → + Chart** → chọn dataset, loại chart (vd: Time-series line, Bar, Table).
- **Time Column:** chọn `created_at` hoặc `order_date`.
- **Refresh:** Trong dashboard có thể đặt **Auto-refresh** (vd: 30 giây) để gần realtime.

### Bước 5 — Tạo dashboard

- **Dashboards → + Dashboard** → đặt tên (vd: “Orders Realtime”).
- Thêm chart đã tạo; sắp xếp, lưu.
- Bật **Auto-refresh** (vd: 30s) để dữ liệu mới (INSERT từ app hoặc CDC) cập nhật định kỳ.

### Gợi ý “realtime”

- Dữ liệu mới: INSERT vào `app.orders` (trực tiếp hoặc qua CDC → Kafka → consumer ghi lại DB). Dashboard Superset refresh theo chu kỳ → cảm nhận near real-time.
- Có thể tạo view `app.orders_recent` (vd: `WHERE created_at > NOW() - INTERVAL '1 day'`) và dùng làm dataset để dashboard nhẹ hơn.

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Flink Web UI | http://localhost:8081 |
| Superset | http://localhost:8088 (admin / admin) |
| Pipeline Python | `week07/stream_kafka_to_minio.py` (topic: lab-events → MinIO bronze/stream/) |
| Flink SQL mẫu | `week07/flink_kafka_to_minio.sql` |
| Download Flink Kafka JAR | `scripts/download_flink_connectors.sh` |
| Postgres URI (Superset, trong Docker) | `postgresql://de_user:de_pass@postgres:5432/de_db` |
