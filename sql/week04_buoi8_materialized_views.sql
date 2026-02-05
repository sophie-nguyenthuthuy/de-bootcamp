-- ============================================================================
-- WEEK 04 · BUỔI 8 — MATERIALIZED VIEWS CHO DASHBOARD
-- Pre-aggregated data for fast Metabase dashboards
-- Chạy: psql ... -f sql/week04_buoi8_materialized_views.sql
-- Lần đầu: tạo view. Cập nhật dữ liệu: REFRESH MATERIALIZED VIEW (cuối file).
-- ============================================================================

SET client_min_messages TO WARNING;

-- ----------------------------------------------------------------------------
-- 1. Doanh thu theo ngày (app.orders)
-- ----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS app.mv_daily_revenue CASCADE;
CREATE MATERIALIZED VIEW app.mv_daily_revenue AS
SELECT
  order_date AS report_date,
  COUNT(DISTINCT order_id) AS num_orders,
  COUNT(*) AS num_items,
  SUM(total_amount) AS revenue
FROM app.orders
WHERE total_amount IS NOT NULL
GROUP BY order_date
ORDER BY order_date DESC;

COMMENT ON MATERIALIZED VIEW app.mv_daily_revenue IS 'Doanh thu theo ngày — dùng cho dashboard';

-- ----------------------------------------------------------------------------
-- 2. Top khách hàng theo tổng tiền (app.orders + app.customers)
-- ----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS app.mv_top_customers CASCADE;
CREATE MATERIALIZED VIEW app.mv_top_customers AS
SELECT
  c.customer_id,
  c.full_name,
  c.email,
  COUNT(o.order_id) AS order_count,
  SUM(o.total_amount) AS total_spent
FROM app.customers c
LEFT JOIN app.orders o ON o.customer_id = c.customer_id AND o.total_amount IS NOT NULL
GROUP BY c.customer_id, c.full_name, c.email
ORDER BY total_spent DESC NULLS LAST;

COMMENT ON MATERIALIZED VIEW app.mv_top_customers IS 'Top khách theo tổng tiền — dashboard';

-- ----------------------------------------------------------------------------
-- 3. Doanh thu theo category (app.order_items + app.products)
-- ----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS app.mv_revenue_by_category CASCADE;
CREATE MATERIALIZED VIEW app.mv_revenue_by_category AS
SELECT
  p.category,
  COUNT(DISTINCT oi.order_id) AS order_count,
  SUM(oi.quantity) AS quantity_sold,
  SUM(oi.subtotal) AS revenue
FROM app.order_items oi
JOIN app.products p ON p.product_id = oi.product_id
WHERE oi.subtotal IS NOT NULL
GROUP BY p.category
ORDER BY revenue DESC;

COMMENT ON MATERIALIZED VIEW app.mv_revenue_by_category IS 'Doanh thu theo category — dashboard';

-- ----------------------------------------------------------------------------
-- REFRESH (chạy sau mỗi lần ETL cập nhật app)
-- ----------------------------------------------------------------------------
REFRESH MATERIALIZED VIEW app.mv_daily_revenue;
REFRESH MATERIALIZED VIEW app.mv_top_customers;
REFRESH MATERIALIZED VIEW app.mv_revenue_by_category;
