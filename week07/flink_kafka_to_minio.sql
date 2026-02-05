-- ============================================================================
-- WEEK 07 · BUỔI 14 — Flink SQL: Kafka → MinIO (S3)
-- Chạy trong Flink SQL Client sau khi thêm JAR:
--   flink-sql-connector-kafka-3.2.0-1.18.jar
--   (và cấu hình S3/MinIO nếu dùng filesystem sink)
-- Bootstrap Kafka (trong Docker): kafka:9092
-- ============================================================================

-- Nguồn: topic lab-events (JSON, không schema cố định → dùng RAW string hoặc define columns)
CREATE TABLE kafka_events (
    `event` STRING,
    `page` STRING,
    `user_id` INT,
    `ts` TIMESTAMP(3),
    WATERMARK FOR `ts` AS `ts` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'lab-events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- Đích: S3/MinIO (cần cấu hình fs.s3.endpoint = http://minio:9000, path-style, credentials)
-- Ví dụ với Flink filesystem connector (path s3://... cần Hadoop/S3 config trong flink-conf)
CREATE TABLE s3_sink (
    `event` STRING,
    `page` STRING,
    `user_id` INT,
    `ts` TIMESTAMP(3)
) WITH (
    'connector' = 'filesystem',
    'path' = 's3://lakehouse/bronze/flink/',
    'format' = 'json'
);

-- Job streaming: copy từ Kafka sang S3
-- INSERT INTO s3_sink SELECT * FROM kafka_events;

-- Lưu ý:
-- 1. Topic lab-events phải có message JSON với key event, page, user_id, ts (hoặc chỉnh lại schema).
-- 2. Ghi ra S3/MinIO từ Flink cần: fs.s3.endpoint=http://minio:9000, fs.s3.path-style.access=true,
--    và credentials (env AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY hoặc trong config).
-- 3. Nếu dùng format khác (vd CSV) thì đổi 'format' = 'csv'.
