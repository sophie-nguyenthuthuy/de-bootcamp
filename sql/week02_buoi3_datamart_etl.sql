-- ============================================================================
-- WEEK 02 · BUỔI 3 — DATA MART ETL
-- Populate mart từ app (dim_date, dim_customer SCD2, dim_product SCD2, fact_sales)
-- Chạy sau: week02_buoi3_datamart_ddl.sql
-- Chạy: psql ... -f sql/week02_buoi3_datamart_etl.sql
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. DIM_DATE — Sinh bảng ngày (vd: 2 năm quanh khoảng order_date trong app)
-- ----------------------------------------------------------------------------
INSERT INTO mart.dim_date (date_key, date_actual, year, month, day_of_week, day_of_month, quarter, year_month)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d AS date_actual,
    EXTRACT(YEAR FROM d)::SMALLINT AS year,
    EXTRACT(MONTH FROM d)::SMALLINT AS month,
    EXTRACT(ISODOW FROM d)::SMALLINT AS day_of_week,
    EXTRACT(DAY FROM d)::SMALLINT AS day_of_month,
    EXTRACT(QUARTER FROM d)::SMALLINT AS quarter,
    TO_CHAR(d, 'YYYYMM')::INT AS year_month
FROM generate_series(
    COALESCE((SELECT MIN(order_date) FROM app.orders), CURRENT_DATE) - INTERVAL '1 year',
    COALESCE((SELECT MAX(order_date) FROM app.orders), CURRENT_DATE) + INTERVAL '1 year',
    '1 day'::INTERVAL
) AS d
ON CONFLICT (date_key) DO NOTHING;

-- ----------------------------------------------------------------------------
-- 2. DIM_CUSTOMER (SCD2) — Load "current" row cho mỗi customer
--    (Lần chạy đầu: mỗi customer 1 row, valid_to = 9999-12-31, is_current = true)
-- ----------------------------------------------------------------------------
INSERT INTO mart.dim_customer (customer_id, full_name, email, phone_number, valid_from, valid_to, is_current)
SELECT
    customer_id,
    full_name,
    email,
    phone_number,
    COALESCE(created_at::DATE, CURRENT_DATE) AS valid_from,
    '9999-12-31'::DATE AS valid_to,
    TRUE AS is_current
FROM app.customers
ON CONFLICT (customer_id, valid_from) DO NOTHING;

-- ----------------------------------------------------------------------------
-- 3. DIM_PRODUCT (SCD2) — Load "current" row cho mỗi product
-- ----------------------------------------------------------------------------
INSERT INTO mart.dim_product (product_id, product_name, category, price, valid_from, valid_to, is_current)
SELECT
    product_id,
    product_name,
    category,
    price,
    COALESCE(created_at::DATE, CURRENT_DATE) AS valid_from,
    '9999-12-31'::DATE AS valid_to,
    TRUE AS is_current
FROM app.products
ON CONFLICT (product_id, valid_from) DO NOTHING;

-- ----------------------------------------------------------------------------
-- 4. FACT_SALES — Grain = 1 row / 1 order_item
--    JOIN app → lấy date_key, customer_sk, product_sk (current), quantity, amount
-- ----------------------------------------------------------------------------
INSERT INTO mart.fact_sales (order_item_id, order_id, date_key, customer_sk, product_sk, quantity, amount, order_date)
SELECT
    oi.order_item_id,
    o.order_id,
    TO_CHAR(o.order_date, 'YYYYMMDD')::INT AS date_key,
    dc.customer_sk,
    dp.product_sk,
    oi.quantity,
    oi.subtotal AS amount,
    o.order_date
FROM app.order_items oi
JOIN app.orders o ON o.order_id = oi.order_id
JOIN app.customers c ON c.customer_id = o.customer_id
JOIN app.products p ON p.product_id = oi.product_id
JOIN mart.dim_customer dc ON dc.customer_id = c.customer_id AND dc.is_current = TRUE
JOIN mart.dim_product dp ON dp.product_id = p.product_id AND dp.is_current = TRUE
ON CONFLICT (order_item_id) DO NOTHING;

-- ----------------------------------------------------------------------------
-- Kiểm tra nhanh
-- ----------------------------------------------------------------------------
-- SELECT 'dim_date' AS tbl, COUNT(*) FROM mart.dim_date
-- UNION ALL SELECT 'dim_customer', COUNT(*) FROM mart.dim_customer
-- UNION ALL SELECT 'dim_product', COUNT(*) FROM mart.dim_product
-- UNION ALL SELECT 'fact_sales', COUNT(*) FROM mart.fact_sales;
