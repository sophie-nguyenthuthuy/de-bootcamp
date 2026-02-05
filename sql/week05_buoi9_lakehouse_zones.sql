-- ============================================================================
-- WEEK 05 · BUỔI 9 — LAKEHOUSE 3 ZONES (bronze / silver / gold)
-- Chạy trong Trino UI (http://localhost:8080) hoặc Trino CLI.
-- Không chạy bằng psql — đây là SQL cho Trino (Hive catalog).
-- ============================================================================

-- Bronze: raw / landing data (CSV, JSON, ...)
CREATE SCHEMA IF NOT EXISTS hive.bronze
WITH (location = 's3a://lakehouse/bronze/');

-- Silver: cleaned, normalized data (e.g. Parquet)
CREATE SCHEMA IF NOT EXISTS hive.silver
WITH (location = 's3a://lakehouse/silver/');

-- Gold: aggregated data for analytics / BI
CREATE SCHEMA IF NOT EXISTS hive.gold
WITH (location = 's3a://lakehouse/gold/');

-- Kiểm tra
SHOW SCHEMAS FROM hive;

-- (Tuỳ chọn) Ví dụ bảng external trong bronze — bỏ comment khi đã có file trong S3
-- CREATE TABLE IF NOT EXISTS hive.bronze.orders (
--     order_id      integer,
--     customer_id   integer,
--     order_date    date,
--     total_amount  double
-- )
-- WITH (
--     format = 'CSV',
--     skip_header_line_count = 1,
--     external_location = 's3a://lakehouse/bronze/orders/'
-- );
