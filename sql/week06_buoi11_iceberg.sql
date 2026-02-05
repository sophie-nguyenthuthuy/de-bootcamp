-- ============================================================================
-- WEEK 06 · BUỔI 11 — Iceberg: partition, vacuum, optimize
-- Chạy trong Trino UI (http://localhost:8080). Catalog: iceberg (Hive Metastore + MinIO).
-- Sau khi thêm trino/etc/catalog/iceberg.properties cần: docker compose restart trino
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Schema + bảng partition (month + category)
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS iceberg.lakehouse
WITH (location = 's3a://lakehouse/iceberg/');

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

-- ----------------------------------------------------------------------------
-- 2. Insert (nhiều batch → nhiều snapshot)
-- ----------------------------------------------------------------------------
INSERT INTO iceberg.lakehouse.sales_events
VALUES
  (1, TIMESTAMP '2024-01-15 10:00:00', 'A', 100.0),
  (2, TIMESTAMP '2024-01-16 11:00:00', 'B', 200.0),
  (3, TIMESTAMP '2024-02-10 12:00:00', 'A', 150.0);

INSERT INTO iceberg.lakehouse.sales_events
VALUES
  (4, TIMESTAMP '2024-02-20 09:00:00', 'B', 250.0),
  (5, TIMESTAMP '2024-03-01 14:00:00', 'A', 300.0);

-- ----------------------------------------------------------------------------
-- 3. Metadata: snapshots, history
-- ----------------------------------------------------------------------------
SELECT snapshot_id, parent_id, operation, committed_at
FROM iceberg.lakehouse."sales_events$snapshots"
ORDER BY committed_at DESC;

SELECT * FROM iceberg.lakehouse."sales_events$history";

-- ----------------------------------------------------------------------------
-- 4. Vacuum: expire snapshots (giữ 7 ngày)
-- ----------------------------------------------------------------------------
ALTER TABLE iceberg.lakehouse.sales_events
EXECUTE expire_snapshots(retention_threshold => '7d');

-- ----------------------------------------------------------------------------
-- 5. Remove orphan files
-- ----------------------------------------------------------------------------
ALTER TABLE iceberg.lakehouse.sales_events
EXECUTE remove_orphan_files(retention_threshold => '7d');

-- ----------------------------------------------------------------------------
-- 6. Time travel (thay <snapshot_id> bằng ID từ "sales_events$snapshots")
-- ----------------------------------------------------------------------------
-- SELECT * FROM iceberg.lakehouse.sales_events FOR VERSION AS OF <snapshot_id>;
-- SELECT * FROM iceberg.lakehouse.sales_events FOR TIMESTAMP AS OF TIMESTAMP '2024-01-16 00:00:00';

-- ============================================================================
-- LAB 2: Optimize large dataset
-- ============================================================================

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

INSERT INTO iceberg.lakehouse.events_large VALUES (1, TIMESTAMP '2024-06-01 08:00:00', 10.0);
INSERT INTO iceberg.lakehouse.events_large VALUES (2, TIMESTAMP '2024-06-01 09:00:00', 20.0);
INSERT INTO iceberg.lakehouse.events_large VALUES (3, TIMESTAMP '2024-06-01 10:00:00', 30.0);
INSERT INTO iceberg.lakehouse.events_large VALUES (4, TIMESTAMP '2024-06-02 08:00:00', 40.0);
INSERT INTO iceberg.lakehouse.events_large VALUES (5, TIMESTAMP '2024-06-02 09:00:00', 50.0);

-- Số file trước optimize
SELECT file_path, record_count, file_size_in_bytes
FROM iceberg.lakehouse."events_large$files";

-- Compact (gộp file nhỏ)
ALTER TABLE iceberg.lakehouse.events_large
EXECUTE optimize(file_size_threshold => '128MB');

-- Optimize manifests (gộp manifest theo partition)
ALTER TABLE iceberg.lakehouse.events_large
EXECUTE optimize_manifests;

-- Số file sau optimize
SELECT file_path, record_count, file_size_in_bytes
FROM iceberg.lakehouse."events_large$files";

-- Compact một partition cụ thể (ví dụ)
-- ALTER TABLE iceberg.lakehouse.events_large
-- EXECUTE optimize(file_size_threshold => '128MB')
-- WHERE ts >= TIMESTAMP '2024-06-01' AND ts < TIMESTAMP '2024-06-02';
