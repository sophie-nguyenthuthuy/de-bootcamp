-- ============================================================================
-- WEEK 02 · BUỔI 3 — DATA MART DDL
-- Star schema: dim_date, dim_customer (SCD2), dim_product (SCD2), fact_sales
-- Chạy: psql ... -f sql/week02_buoi3_datamart_ddl.sql
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- ============================================================================
-- DIM_DATE — Dimension thời gian (bảng tham chiếu)
-- ============================================================================
CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_key     INT PRIMARY KEY,           -- YYYYMMDD
    date_actual  DATE NOT NULL UNIQUE,
    year         SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    day_of_week  SMALLINT NOT NULL,          -- 1=Mon .. 7=Sun
    day_of_month SMALLINT NOT NULL,
    quarter      SMALLINT NOT NULL,
    year_month   INT NOT NULL                -- YYYYMM
);

COMMENT ON TABLE mart.dim_date IS 'Dimension date — dùng cho phân tích theo ngày/tháng/năm';

-- ============================================================================
-- DIM_CUSTOMER — SCD Type 2 (lưu lịch sử thay đổi)
-- ============================================================================
CREATE TABLE IF NOT EXISTS mart.dim_customer (
    customer_sk   SERIAL PRIMARY KEY,       -- Surrogate key
    customer_id   INT NOT NULL,              -- Natural key (app.customers.customer_id)
    full_name     VARCHAR(255) NOT NULL,
    email         VARCHAR(255),
    phone_number  VARCHAR(20),
    valid_from    DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to      DATE NOT NULL DEFAULT '9999-12-31',
    is_current    BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (customer_id, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_id_current
ON mart.dim_customer(customer_id, is_current) WHERE is_current = TRUE;

COMMENT ON TABLE mart.dim_customer IS 'Dimension customer SCD2 — mỗi thay đổi = 1 row mới, valid_from/valid_to';

-- ============================================================================
-- DIM_PRODUCT — SCD Type 2
-- ============================================================================
CREATE TABLE IF NOT EXISTS mart.dim_product (
    product_sk   SERIAL PRIMARY KEY,
    product_id   INT NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category     VARCHAR(100),
    price        NUMERIC(10,2) NOT NULL,
    valid_from   DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to     DATE NOT NULL DEFAULT '9999-12-31',
    is_current   BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (product_id, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_dim_product_id_current
ON mart.dim_product(product_id, is_current) WHERE is_current = TRUE;

COMMENT ON TABLE mart.dim_product IS 'Dimension product SCD2 — lịch sử giá/category theo thời gian';

-- ============================================================================
-- FACT_SALES — Grain: 1 row = 1 order_item
-- ============================================================================
CREATE TABLE IF NOT EXISTS mart.fact_sales (
    fact_sales_id  SERIAL PRIMARY KEY,
    order_item_id  INT NOT NULL UNIQUE,      -- Grain
    order_id       INT NOT NULL,
    date_key       INT NOT NULL REFERENCES mart.dim_date(date_key),
    customer_sk    INT NOT NULL REFERENCES mart.dim_customer(customer_sk),
    product_sk     INT NOT NULL REFERENCES mart.dim_product(product_sk),
    quantity       INT NOT NULL,
    amount         NUMERIC(12,2) NOT NULL,   -- subtotal
    order_date     DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON mart.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON mart.fact_sales(customer_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON mart.fact_sales(product_sk);

COMMENT ON TABLE mart.fact_sales IS 'Fact sales — grain = order_item; measure: quantity, amount';
