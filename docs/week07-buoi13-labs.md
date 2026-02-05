# Week 7 · Buổi 13 — CDC with Debezium

**Change Data Capture · Debezium (binlog, WAL) · Lab: CDC PostgreSQL → Kafka · Lab: Stream change events to bronze**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Lý thuyết | **Change Data Capture (CDC)** — capture thay đổi từ DB |
| Lý thuyết | **Debezium: binlog, WAL** — logical decoding, pgoutput |
| Lab 1 | **CDC PostgreSQL → Kafka** — Debezium connector, topic `dbserver1.app.orders` |
| Lab 2 | **Stream change events to bronze** — consumer ghi CDC events vào MinIO bronze |

---

## Chuẩn bị

- Docker đang chạy.
- Stack streaming đã chạy: `docker compose --profile streaming up -d` (Zookeeper, Kafka, Connect).
- Postgres đã có schema `app` và bảng `app.orders` (chạy `sql/01_create_oltp_schema.sql` + seed nếu cần).
- Postgres đã bật **WAL logical** (`wal_level=logical`) trong `docker-compose.yml` (đã cấu hình sẵn).

---

## Lý thuyết tóm tắt

### Change Data Capture (CDC)

- **CDC:** Kỹ thuật capture mọi thay đổi (INSERT/UPDATE/DELETE) từ database và phát ra stream (vd: Kafka) gần real-time.
- **Lợi ích:** Đồng bộ data sang data lake/warehouse, event-driven, audit, replay.
- **Cách làm:** Trigger-based (trigger ghi log) hoặc **log-based** (đọc transaction log: MySQL binlog, PostgreSQL WAL).

### Debezium: binlog, WAL

- **Debezium:** Nguồn mở, platform CDC; đọc log giao dịch của DB và gửi event vào Kafka.
- **MySQL:** Đọc **binlog** (binary log).
- **PostgreSQL:** Đọc **WAL** (Write-Ahead Log) qua **logical decoding**; plugin mặc định dùng **pgoutput** (Postgres 10+).
- **Event:** Mỗi thay đổi → message Kafka (key = primary key, value = envelope: before/after, op, ts_ms, …).

### Luồng CDC trong lab

```
PostgreSQL (app.orders)  →  WAL  →  Debezium Connect  →  Kafka topic (dbserver1.app.orders)
                                                                  ↓
                                                    Consumer  →  MinIO bronze (JSON)
```

---

## Lab 1: CDC PostgreSQL → Kafka

**Mục tiêu:** Cấu hình Debezium PostgreSQL connector, INSERT/UPDATE vào `app.orders`, xác nhận event xuất hiện trên Kafka topic.

### Bước 1 — Khởi động stack streaming (Kafka + Connect)

```bash
cd de-bootcamp
docker compose --profile streaming up -d
```

Đợi Zookeeper, Kafka, Connect sẵn sàng (30–60 giây). Connect chạy trên port **8083**.

### Bước 2 — Đảm bảo Postgres có schema và bảng

Nếu chưa có:

```bash
docker exec -i de_postgres psql -U de_user -d de_db < sql/01_create_oltp_schema.sql
```

Kiểm tra:

```sql
\dt app.*
-- Cần thấy app.orders
```

### Bước 3 — Đăng ký Debezium PostgreSQL connector

Từ máy host (Connect REST API trên localhost:8083):

```bash
curl -s -X POST -H "Content-Type: application/json" \
  --data @scripts/debezium-postgres-connector.json \
  http://localhost:8083/connectors
```

Hoặc inline (nếu không dùng file):

```bash
curl -s -X POST -H "Content-Type: application/json" \
  --data '{
    "name": "debezium-postgres-orders",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "de_user",
      "database.password": "de_pass",
      "database.dbname": "de_db",
      "topic.prefix": "dbserver1",
      "plugin.name": "pgoutput",
      "table.include.list": "app.orders",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter"
    }
  }' \
  http://localhost:8083/connectors
```

Kết quả mong đợi: `{"name":"debezium-postgres-orders",...}`.

### Bước 4 — Kiểm tra connector

```bash
curl -s http://localhost:8083/connectors
curl -s http://localhost:8083/connectors/debezium-postgres-orders/status
```

Trạng thái runner nên là **RUNNING**.

### Bước 5 — Tạo dữ liệu thay đổi trong Postgres

```sql
INSERT INTO app.orders (customer_id, order_date, status)
VALUES (1, CURRENT_DATE, 'created');
```

(Cần có `customer_id` 1 trong `app.customers`; nếu chưa có thì insert customer trước.)

### Bước 6 — Đọc CDC topic từ Kafka

```bash
docker exec -it de_kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic dbserver1.app.orders \
  --from-beginning
```

Sẽ thấy message JSON (envelope Debezium: `before`, `after`, `op`, `source`, …) tương ứng INSERT vừa thực hiện.

### Bước 7 — (Tuỳ chọn) UPDATE và quan sát event

```sql
UPDATE app.orders SET status = 'confirmed' WHERE order_id = 1;
```

Consumer lại topic sẽ thấy thêm event cho UPDATE.

---

## Lab 2: Stream change events to bronze layer

**Mục tiêu:** Consumer đọc từ topic CDC `dbserver1.app.orders` và ghi từng event (hoặc batch) vào MinIO bucket **bronze** dưới dạng JSON — tầng bronze = raw CDC events.

### Mô hình

- **Kafka topic:** `dbserver1.app.orders` (Debezium envelope).
- **Bronze:** MinIO bucket `lakehouse`, prefix `bronze/cdc/orders/` — mỗi file JSON một event (hoặc batch theo giờ/ngày).

### Bước 1 — Chạy script Python `cdc_to_bronze.py`

Script trong `week07/`:

- Consume từ `dbserver1.app.orders` (group `cdc-bronze`).
- Ghi mỗi message value (JSON) lên MinIO: `lakehouse/bronze/cdc/orders/` với object key có timestamp (vd: `YYYY-MM-DD/HH-mm-ss-offset.json`).

**Cài dependency:**

```bash
cd de-bootcamp/week07
pip install -r requirements.txt
```

**Chạy consumer (MinIO và Kafka đang chạy):**

```bash
python cdc_to_bronze.py
```

Giữ terminal mở; script chạy liên tục, mỗi event CDC ghi một file lên MinIO.

### Bước 2 — Tạo thay đổi trong Postgres

Ở terminal/SQL khác:

```sql
INSERT INTO app.orders (customer_id, order_date, status)
VALUES (1, CURRENT_DATE, 'shipped');
```

### Bước 3 — Kiểm tra file trên MinIO

- MinIO Console: http://localhost:9001 → bucket `lakehouse` → prefix `bronze/cdc/orders/`.
- Hoặc dùng mc: `mc ls local/lakehouse/bronze/cdc/orders/`.

Sẽ thấy file JSON tương ứng event CDC vừa gửi.

### Cấu trúc thư mục (bronze)

```
s3a://lakehouse/bronze/cdc/orders/
  2025-02-04/
    12-30-45-0001.json
    12-31-00-0002.json
```

Mỗi file là nội dung value của message Kafka (Debezium envelope).

---

## Lưu ý

- **Postgres lần đầu chạy với `wal_level=logical`:** Nếu volume Postgres đã tồn tại từ trước khi thêm `command: ["postgres", "-c", "wal_level=logical"]`, cần đảm bảo server được khởi động lại với tham số này; nếu cần, tạo DB mới hoặc chỉnh `postgresql.conf` trong volume.
- **Connect và Postgres cùng network:** Connector config dùng `database.hostname=postgres` — đúng khi Connect chạy trong Docker cùng compose với Postgres.
- **Topic naming:** `topic.prefix` = `dbserver1` → topic = `dbserver1.app.orders` (schema.table).

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Connect REST API | http://localhost:8083 |
| Connector name | `debezium-postgres-orders` |
| Topic CDC | `dbserver1.app.orders` |
| Đăng ký connector | `curl -X POST -H "Content-Type: application/json" --data @scripts/debezium-postgres-connector.json http://localhost:8083/connectors` |
| Bronze path (MinIO) | `lakehouse/bronze/cdc/orders/` |
| Script bronze | `week07/cdc_to_bronze.py` |
