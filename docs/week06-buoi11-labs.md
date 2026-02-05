# Week 6 · Buổi 11 — Table Format & Optimization

**Partitioning · Compaction · Z-order · ACID · Lab: Iceberg partition + vacuum · Lab: Optimize large dataset**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Lý thuyết | **Partitioning** — partition by column/transform, prune |
| Lý thuyết | **Compaction, Z-order** — merge small files, sort order |
| Lý thuyết | **ACID transaction** — snapshot, time travel |
| Lab 1 | **Iceberg: partition + vacuum** — bảng partition, expire_snapshots, remove_orphan_files |
| Lab 2 | **Optimize large dataset** — optimize (compact), optimize_manifests |

---

## Chuẩn bị

- Docker đang chạy; stack MinIO + Hive Metastore + Trino đã chạy (Week 5).
- Catalog **iceberg** đã cấu hình trong Trino (file `trino/etc/catalog/iceberg.properties`). Sau khi thêm file, cần **restart Trino** để load catalog mới.

---

## Lý thuyết tóm tắt

### Partitioning

- **Mục đích:** Giảm dữ liệu đọc khi query có filter theo cột partition (partition pruning).
- **Cách chọn:** Partition theo cột có cardinality vừa phải, thường dùng trong WHERE (vd: `date`, `region`, `year(ts)`).
- **Iceberg:** Hỗ trợ identity (`col`) và transform: `year(ts)`, `month(ts)`, `day(ts)`, `hour(ts)`, `bucket(col, n)`, `truncate(s, n)`.

### Compaction & Z-order

- **Compaction:** Gộp nhiều file nhỏ thành ít file lớn hơn → giảm số file đọc, cải thiện I/O.
- **Z-order (sort order):** Sắp xếp dữ liệu trong file theo cột (vd: `sorted_by`) → tận dụng predicate pushdown, giảm scan.
- **Iceberg trong Trino:** `ALTER TABLE ... EXECUTE optimize(...)` để compact; table property `sorted_by` để chỉ định thứ tự sort trong file.

### ACID transaction

- **Snapshot:** Mỗi lần commit (INSERT/UPDATE/DELETE) tạo snapshot mới; metadata lưu danh sách file.
- **Time travel:** Query bảng tại snapshot cũ hoặc thời điểm trong quá khứ: `FOR VERSION AS OF <id>`, `FOR TIMESTAMP AS OF <ts>`.
- **Rollback:** Có thể rollback về snapshot trước bằng procedure `rollback_to_snapshot`.

### Vacuum / maintenance

- **expire_snapshots:** Xóa snapshot và metadata cũ (giữ retention, vd 7 ngày) → thu hồi data files không còn tham chiếu.
- **remove_orphan_files:** Xóa file trong thư mục bảng không còn nằm trong metadata (file “mồ côi” sau job lỗi, v.v.).
- **optimize (compact):** Gộp file nhỏ trong từng partition.
- **optimize_manifests:** Gộp manifest file theo partition → tối ưu planning khi có partition filter.

---

## Lab 1: Iceberg — partition + vacuum

**Mục tiêu:** Tạo bảng Iceberg có partition, ghi dữ liệu, rồi chạy expire_snapshots và remove_orphan_files (tương tự “vacuum” trong Delta).

### Bước 0 — Bật catalog Iceberg và restart Trino

1. Đảm bảo file `trino/etc/catalog/iceberg.properties` tồn tại (cùng thư mục với `hive.properties`).
2. Restart Trino để load catalog:

```bash
docker compose restart trino
```

Đợi Trino lên (30–60 giây), vào http://localhost:8080 → Execute SQL:

```sql
SHOW CATALOGS;
-- Cần thấy: iceberg, hive, ...
SHOW SCHEMAS FROM iceberg;
```

### Bước 1 — Tạo schema Iceberg trên S3 (MinIO)

Chạy trong Trino:

```sql
CREATE SCHEMA IF NOT EXISTS iceberg.lakehouse
WITH (location = 's3a://lakehouse/iceberg/');

SHOW SCHEMAS FROM iceberg;
```

### Bước 2 — Tạo bảng partition (vd: theo tháng và category)

```sql
CREATE TABLE IF NOT EXISTS iceberg.lakehouse.sales_events (
    event_id     BIGINT,
    event_time   TIMESTAMP(6),
    category     VARCHAR,
    amount       DOUBLE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['month(event_time)', 'category'],
    location = 's3a://lakehouse/iceberg/sales_events/'
);
```

### Bước 3 — Insert dữ liệu (nhiều lần để có nhiều snapshot)

```sql
INSERT INTO iceberg.lakehouse.sales_events
VALUES
  (1, TIMESTAMP '2024-01-15 10:00:00', 'A', 100.0),
  (2, TIMESTAMP '2024-01-16 11:00:00', 'B', 200.0),
  (3, TIMESTAMP '2024-02-10 12:00:00', 'A', 150.0);

INSERT INTO iceberg.lakehouse.sales_events
VALUES
  (4, TIMESTAMP '2024-02-20 09:00:00', 'B', 250.0),
  (5, TIMESTAMP '2024-03-01 14:00:00', 'A', 300.0);
```

### Bước 4 — Xem snapshot và history

```sql
SELECT snapshot_id, parent_id, operation, committed_at
FROM iceberg.lakehouse."sales_events$snapshots"
ORDER BY committed_at DESC;

SELECT * FROM iceberg.lakehouse."sales_events$history";
```

### Bước 5 — Expire snapshots (vacuum cũ)

Chỉ giữ snapshot trong 1 ngày (min retention catalog mặc định 7d; có thể giữ 7d hoặc chỉnh catalog property):

```sql
-- Giữ snapshot 7 ngày (mặc định min retention)
ALTER TABLE iceberg.lakehouse.sales_events
EXECUTE expire_snapshots(retention_threshold => '7d');
```

Nếu catalog cho phép retention ngắn hơn (vd `iceberg.expire-snapshots.min-retention=1d`), có thể dùng `retention_threshold => '1d'`.

### Bước 6 — Remove orphan files

```sql
ALTER TABLE iceberg.lakehouse.sales_events
EXECUTE remove_orphan_files(retention_threshold => '7d');
```

Output có các metric: `processed_manifests_count`, `deleted_files_count`, …

### Bước 7 — (Tuỳ chọn) Time travel

```sql
-- Lấy snapshot_id từ bước 4
SELECT * FROM iceberg.lakehouse.sales_events
FOR VERSION AS OF <snapshot_id>;

-- Hoặc theo thời điểm
SELECT * FROM iceberg.lakehouse.sales_events
FOR TIMESTAMP AS OF TIMESTAMP '2024-01-16 00:00:00';
```

---

## Lab 2: Optimize large dataset

**Mục tiêu:** Compact file nhỏ (optimize) và tối ưu manifest (optimize_manifests) cho bảng Iceberg.

### Bước 1 — Tạo bảng có nhiều file nhỏ (insert nhiều lần)

Dùng lại `iceberg.lakehouse.sales_events` hoặc tạo bảng mới:

```sql
CREATE TABLE IF NOT EXISTS iceberg.lakehouse.events_large (
    id        BIGINT,
    ts        TIMESTAMP(6),
    value     DOUBLE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(ts)'],
    location = 's3a://lakehouse/iceberg/events_large/'
);

-- Insert nhiều batch nhỏ → nhiều file
INSERT INTO iceberg.lakehouse.events_large VALUES (1, TIMESTAMP '2024-06-01 08:00:00', 10.0);
INSERT INTO iceberg.lakehouse.events_large VALUES (2, TIMESTAMP '2024-06-01 09:00:00', 20.0);
INSERT INTO iceberg.lakehouse.events_large VALUES (3, TIMESTAMP '2024-06-01 10:00:00', 30.0);
INSERT INTO iceberg.lakehouse.events_large VALUES (4, TIMESTAMP '2024-06-02 08:00:00', 40.0);
INSERT INTO iceberg.lakehouse.events_large VALUES (5, TIMESTAMP '2024-06-02 09:00:00', 50.0);
```

### Bước 2 — Xem số file hiện tại

```sql
SELECT file_path, record_count, file_size_in_bytes
FROM iceberg.lakehouse."events_large$files";
```

### Bước 3 — Compact (optimize)

Gộp file nhỏ hơn 128MB trong toàn bảng:

```sql
ALTER TABLE iceberg.lakehouse.events_large
EXECUTE optimize(file_size_threshold => '128MB');
```

Chỉ compact một partition:

```sql
ALTER TABLE iceberg.lakehouse.events_large
EXECUTE optimize(file_size_threshold => '128MB')
WHERE ts >= TIMESTAMP '2024-06-01' AND ts < TIMESTAMP '2024-06-02';
```

### Bước 4 — Optimize manifests

Gộp manifest theo partition để query có partition filter nhanh hơn:

```sql
ALTER TABLE iceberg.lakehouse.events_large
EXECUTE optimize_manifests;
```

### Bước 5 — Kiểm tra lại số file

```sql
SELECT file_path, record_count, file_size_in_bytes
FROM iceberg.lakehouse."events_large$files";
```

Sau optimize, số file có thể giảm (nhiều file nhỏ → ít file lớn hơn).

---

## File SQL tham chiếu

- **`sql/week06_buoi11_iceberg.sql`** — Script đầy đủ: tạo schema, bảng partition, insert, expire_snapshots, remove_orphan_files, optimize, optimize_manifests, và truy vấn metadata ($snapshots, $files, $history).

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Trino UI | http://localhost:8080 |
| Catalog Iceberg | `iceberg` (Hive Metastore + S3/MinIO) |
| Schema mẫu | `iceberg.lakehouse` |
| Expire snapshots | `ALTER TABLE t EXECUTE expire_snapshots(retention_threshold => '7d')` |
| Remove orphan | `ALTER TABLE t EXECUTE remove_orphan_files(retention_threshold => '7d')` |
| Compact | `ALTER TABLE t EXECUTE optimize(file_size_threshold => '128MB')` |
| Optimize manifests | `ALTER TABLE t EXECUTE optimize_manifests` |
| Restart Trino sau khi thêm catalog | `docker compose restart trino` |
