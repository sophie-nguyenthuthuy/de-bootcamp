
# de-bootcamp
# DATA ENGINEERING BOOTCAMP – HƯỚNG DẪN SETUP MÔI TRƯỜNG LAB
# 📘 DATA ENGINEERING BOOTCAMP — STUDENT SETUP GUIDE

6–9 Week Program: Big Data, Lakehouse, Spark, Kafka, Airflow, Trino, Data Governance

Repo này chứa toàn bộ hạ tầng cần thiết để học viên tự chạy các lab trong khóa học.

---

## 1. YÊU CẦU MÁY TÍNH

| Component | Recommendation |
|----------|----------------|
| CPU | ≥ 4 vCPU |
| RAM | ≥ 16 GB |
| Disk | ≥ 50 GB |
| OS | macOS / Windows WSL2 / Linux |
| Tools | Docker Desktop, Git, Python 3.10 |

---

## 2. CLONE REPOSITORY

```bash
git clone https://github.com/your-org/de-bootcamp.git
cd de-bootcamp
```

---

## 3. KHỞI CHẠY DOCKER COMPOSE

### Chạy Core (Postgres + MinIO):
```bash
docker compose --profile core up -d
```

### Chạy Lakehouse (Trino + Hive + MinIO):
```bash
docker compose --profile lakehouse --profile spark up -d
```

### Chạy Kafka + Debezium (CDC):
```bash
docker compose --profile streaming up -d
```

### Chạy Airflow:
```bash
docker compose --profile airflow up -d
```

### Chạy toàn bộ stack (nặng):
```bash
docker compose up -d
```

### Dừng tất cả:
```bash
docker compose down
```

---

## 4. KIỂM TRA DỊCH VỤ

### Postgres
```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db
```

### MinIO Console
```
http://localhost:9001
User: minioadmin
Pass: minioadmin
```

### Trino UI
```
http://localhost:8080
```

### Airflow UI
```
http://localhost:8088
User: admin
Pass: admin
```

### Metabase
```
http://localhost:3000
```

---

## 5. KHỞI TẠO DỮ LIỆU OLTP

### Chạy file tạo schema:
```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/01_create_oltp_schema.sql
```

### Seed dữ liệu mẫu:
```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/02_seed_sample_data.sql
```

---

## 6. TEST TRINO + MINIO + LAKEHOUSE

Mở Trino CLI hoặc web UI → chạy:

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM hive;

CREATE SCHEMA hive.lakehouse 
WITH (location='s3a://lakehouse/');
```

---

## 7. CHẠY DAG AIRFLOW

### Example 1 — Postgres → MinIO (Bronze)

Trong Airflow UI, bật DAG:

```
postgres_to_minio_etl
```

Sau đó kiểm tra file xuất hiện trong MinIO:

- bucket: **lakehouse**
- path: **bronze/orders/orders_extract.csv**

---

## 8. CHẠY SPARK JOB (Bronze → Silver → Gold)

Airflow DAG:

```
spark_bronze_to_silver_gold
```

Job thực hiện:

- Đọc CSV từ MinIO  
- Làm sạch dữ liệu  
- Ghi Silver (Parquet) phân vùng theo ngày  
- Tính Gold metrics (daily revenue, total orders…)  

---

## 9. CẤU TRÚC DỮ LIỆU LAKEHOUSE

```
lakehouse/
├─ raw/
├─ bronze/
│   └─ orders/
├─ silver/
│   └─ orders_clean/
└─ gold/
    └─ orders_daily_metrics/
```

---

## 10. KAFKA + DEBEZIUM (CDC)

Test Debezium:

### Insert vào PostgreSQL:
```sql
INSERT INTO app.orders (customer_id, order_date, status)
VALUES (1, CURRENT_DATE, 'created');
```

### Check topic Kafka:
```
connect-debezium.app.orders
```

Dữ liệu CDC sẽ được stream sang MinIO hoặc Spark Streaming.

---

## 11. SỰ CỐ THƯỜNG GẶP

### Docker bị treo → restart stack:
```bash
docker compose down -v
docker system prune -af
docker compose up -d
```

### Airflow không nhận DAG?
```bash
docker exec -it de_airflow ls /opt/airflow/dags-local
```

### Trino không thấy catalog hive?
```bash
docker compose restart trino
```

---

## 12. LIÊN HỆ / HỖ TRỢ

Nếu cần hỗ trợ trong lúc học, học viên có thể mở issue trong repo hoặc hỏi trực tiếp trong lớp.

---

# 13. PHỤ LỤC – CÂU LỆNH MẪU KIỂM TRA TỪNG LAB

## 13.1 SQL Lab – Kiểm tra schema & dữ liệu OLTP

Sau khi chạy:

```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/01_create_oltp_schema.sql
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/02_seed_sample_data.sql
```
## 13.2 Lakehouse / Trino / MinIO – Kiểm tra hoạt động

Sau khi bật core + lakehouse:

```bash
docker compose --profile core --profile lakehouse up -d
```

Mở Trino UI tại:

```
http://localhost:8080
```

Chạy các lệnh kiểm tra:

```sql
SHOW CATALOGS;

SHOW SCHEMAS FROM hive;

CREATE SCHEMA IF NOT EXISTS hive.lakehouse
WITH (location='s3a://lakehouse/');

SHOW TABLES FROM hive.lakehouse;
```

Nếu bạn đã load file CSV vào MinIO (bronze), tạo bảng external để đọc:

```sql
CREATE TABLE hive.lakehouse.orders_bronze (
    order_id        integer,
    customer_id     integer,
    order_date      date,
    total_amount    double
)
WITH (
    external_location='s3a://lakehouse/bronze/orders/',
    format='CSV',
    skip_header_line_count = 1
);

SELECT * FROM hive.lakehouse.orders_bronze LIMIT 10;
```

---

## 13.3 Spark Lab – Kiểm tra job Bronze → Silver → Gold

Bật lakehouse + spark:

```bash
docker compose --profile lakehouse --profile spark up -d
```

Chạy Spark job trực tiếp qua spark-submit:

```bash
docker exec -it de_spark_master \
  spark-submit \
    --master spark://spark-master:7077 \
    /opt/airflow/spark_jobs/bronze_to_silver_gold.py \
    s3a://lakehouse/bronze/orders/orders_extract.csv \
    s3a://lakehouse/silver/orders_clean/ \
    s3a://lakehouse/gold/orders_daily_metrics/ \
    http://minio:9000 \
    minioadmin \
    minioadmin
```

Sau khi job chạy xong, kiểm tra bằng Trino:

```sql
SELECT * FROM hive.lakehouse.orders_silver LIMIT 10;

SELECT * FROM hive.lakehouse.orders_daily_metrics LIMIT 10;
```

---

## 13.4 Kafka – Kiểm tra Kafka Broker & Topic

Bật Kafka + Zookeeper:

```bash
docker compose --profile streaming up -d
```

Truy cập container Kafka:

```bash
docker exec -it de_kafka bash
```

Tạo topic:

```bash
kafka-topics --create \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic test-topic
```

List topic:

```bash
kafka-topics --list --bootstrap-server kafka:9092
```

Producer test:

```bash
kafka-console-producer --broker-list kafka:9092 --topic test-topic
>hello
>this is a test
```

Consumer:

```bash
kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic test-topic \
  --from-beginning
```

---

## 13.5 CDC (Debezium) – Kiểm tra Connector

Trong terminal:

```bash
curl http://localhost:8083/connectors
```

Kiểm tra trạng thái connector (ví dụ: debezium-postgres-orders):

```bash
curl http://localhost:8083/connectors/debezium-postgres-orders/status
```

Insert thử dữ liệu vào Postgres:

```sql
INSERT INTO app.orders(customer_id, order_date, status)
VALUES (1, CURRENT_DATE, 'created');
```

Consumer CDC topic (tùy connector config):

```bash
kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic dbserver1.app.orders \
  --from-beginning
```

---

## 13.6 Airflow – Kiểm tra DAG & Scheduler

List DAGs:

```bash
docker exec -it de_airflow airflow dags list
```

Show DAG graph:

```bash
docker exec -it de_airflow airflow dags show postgres_to_minio_etl
```

Trigger DAG:

```bash
docker exec -it de_airflow airflow dags trigger postgres_to_minio_etl
```

Xem lịch sử chạy:

```bash
docker exec -it de_airflow airflow dags list-runs postgres_to_minio_etl
```

Kiểm tra logs:

```bash
docker exec -it de_airflow airflow tasks logs postgres_to_minio_etl extract_from_postgres
```

---

## 13.7 Metabase – Kiểm tra kết nối Postgres

Truy cập vào:

```
http://localhost:3000
```

Thêm database mới:

- **Type:** Postgres  
- **Host:** postgres  
- **Port:** 5432  
- **DB name:** de_db  
- **User:** de_user  
- **Password:** de_pass  

Tạo câu hỏi (Question):

```sql
SELECT order_date, total_amount
FROM app.orders
ORDER BY order_date DESC;
```

Nếu query chạy OK → kết nối thành công.

```

