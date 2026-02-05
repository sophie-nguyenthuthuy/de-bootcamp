-- ============================================================================
-- WEEK 09 · BUỔI 17 — Bronze/Silver/Gold trên S3 (MinIO) & Query Athena (Trino)
-- Chạy trong Trino UI (http://localhost:8080). Catalog: hive.
-- Trên AWS: S3 = bucket, Glue = catalog, Athena = query engine.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Đảm bảo zones (nếu chưa có từ Buổi 9)
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS hive.bronze
WITH (location = 's3a://lakehouse/bronze/');

CREATE SCHEMA IF NOT EXISTS hive.silver
WITH (location = 's3a://lakehouse/silver/');

CREATE SCHEMA IF NOT EXISTS hive.gold
WITH (location = 's3a://lakehouse/gold/');

SHOW SCHEMAS FROM hive;

-- ----------------------------------------------------------------------------
-- 2. Query mẫu (Athena tương đương)
-- ----------------------------------------------------------------------------
-- Liệt kê bảng trong bronze
SHOW TABLES FROM hive.bronze;

-- Liệt kê bảng trong silver
SHOW TABLES FROM hive.silver;

-- Liệt kê bảng trong gold
SHOW TABLES FROM hive.gold;

-- (Khi đã có bảng) Ví dụ query bronze.orders
-- SELECT * FROM hive.bronze.orders LIMIT 10;

-- (Khi đã có bảng) Ví dụ query silver.orders_clean
-- SELECT * FROM hive.silver.orders_clean LIMIT 10;

-- (Khi đã có bảng) Ví dụ query gold.orders_daily_metrics
-- SELECT * FROM hive.gold.orders_daily_metrics LIMIT 10;

-- ----------------------------------------------------------------------------
-- 3. Mapping lên AWS
-- ----------------------------------------------------------------------------
-- MinIO s3a://lakehouse/bronze/  →  S3 s3://my-bucket/bronze/
-- Hive catalog (Metastore)       →  Glue Data Catalog
-- Trino (hive.bronze.orders)     →  Athena (database bronze, table orders)
