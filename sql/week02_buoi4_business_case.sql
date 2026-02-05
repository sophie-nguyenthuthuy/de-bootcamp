-- ============================================================================
-- WEEK 02 · BUỔI 4 — BÀI TẬP SQL BUSINESS CASE THỰC TẾ
-- Schema: app (OLTP), mart (data mart Sales)
-- Chạy: psql ... -f sql/week02_buoi4_business_case.sql
-- Hoặc copy từng block vào psql. SET search_path TO app; hoặc dùng mart.* / app.*
-- ============================================================================

-- ----------------------------------------------------------------------------
-- CASE 1: Top 3 khách hàng theo tổng doanh thu (dùng RANK, mart)
-- ----------------------------------------------------------------------------
-- Đề: Liệt kê top 3 khách (full_name) theo tổng amount trong fact_sales.
-- Gợi ý: CTE tổng doanh thu theo customer_sk, RANK() OVER (ORDER BY revenue DESC).

WITH customer_revenue AS (
  SELECT f.customer_sk, SUM(f.amount) AS revenue
  FROM mart.fact_sales f
  GROUP BY f.customer_sk
),
ranked AS (
  SELECT c.full_name, cr.revenue,
         RANK() OVER (ORDER BY cr.revenue DESC) AS rn
  FROM mart.dim_customer c
  JOIN customer_revenue cr ON cr.customer_sk = c.customer_sk
  WHERE c.is_current = TRUE
)
SELECT full_name, revenue, rn
FROM ranked
WHERE rn <= 3
ORDER BY rn;

-- ----------------------------------------------------------------------------
-- CASE 2: Running total doanh thu theo ngày (window SUM OVER, mart)
-- ----------------------------------------------------------------------------
-- Đề: Mỗi ngày có order: order_date, doanh thu ngày đó, running total đến ngày đó.

SELECT
  f.order_date,
  SUM(f.amount) AS daily_revenue,
  SUM(SUM(f.amount)) OVER (ORDER BY f.order_date) AS running_total
FROM mart.fact_sales f
GROUP BY f.order_date
ORDER BY f.order_date;

-- ----------------------------------------------------------------------------
-- CASE 3: % đóng góp doanh thu theo category (CTE + ratio, mart)
-- ----------------------------------------------------------------------------
-- Đề: Mỗi category: product category, revenue, % so với tổng doanh thu.

WITH cat_revenue AS (
  SELECT p.category, SUM(f.amount) AS revenue
  FROM mart.fact_sales f
  JOIN mart.dim_product p ON p.product_sk = f.product_sk
  WHERE p.is_current = TRUE
  GROUP BY p.category
),
total_revenue AS (
  SELECT SUM(revenue) AS total FROM cat_revenue
)
SELECT cr.category, cr.revenue,
       ROUND(100.0 * cr.revenue / tr.total, 2) AS pct_of_total
FROM cat_revenue cr
CROSS JOIN total_revenue tr
ORDER BY cr.revenue DESC;

-- ----------------------------------------------------------------------------
-- CASE 4: Doanh thu theo tháng + so sánh tháng trước (LAG, mart)
-- ----------------------------------------------------------------------------
-- Đề: Mỗi tháng: year, month, revenue, revenue tháng trước, chênh lệch.

WITH monthly AS (
  SELECT d.year, d.month, SUM(f.amount) AS revenue
  FROM mart.fact_sales f
  JOIN mart.dim_date d ON d.date_key = f.date_key
  GROUP BY d.year, d.month
)
SELECT year, month, revenue,
       LAG(revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
       revenue - LAG(revenue) OVER (ORDER BY year, month) AS diff_vs_prev
FROM monthly
ORDER BY year, month;

-- ----------------------------------------------------------------------------
-- CASE 5: Khách có đơn trong 10 ngày qua nhưng không có đơn 11–20 ngày trước (app)
-- ----------------------------------------------------------------------------
-- Đề: Customer_id, full_name của khách có order trong (CURRENT_DATE - 10, CURRENT_DATE)
--     và không có order trong (CURRENT_DATE - 20, CURRENT_DATE - 10].
-- Gợi ý: EXISTS / NOT EXISTS hoặc LEFT JOIN + NULL.

SELECT c.customer_id, c.full_name
FROM app.customers c
WHERE EXISTS (
  SELECT 1 FROM app.orders o
  WHERE o.customer_id = c.customer_id
    AND o.order_date > CURRENT_DATE - INTERVAL '10 days'
    AND o.order_date <= CURRENT_DATE
)
AND NOT EXISTS (
  SELECT 1 FROM app.orders o
  WHERE o.customer_id = c.customer_id
    AND o.order_date > CURRENT_DATE - INTERVAL '20 days'
    AND o.order_date <= CURRENT_DATE - INTERVAL '10 days'
);

-- ----------------------------------------------------------------------------
-- CASE 6: ETL-style — Dedup bằng ROW_NUMBER (vd: 1 row/order_item, latest by updated_at)
-- ----------------------------------------------------------------------------
-- Đề: Giả sử có bảng staging order_items có thể trùng order_item_id (nhiều lần load).
--     Lấy 1 row mới nhất cho mỗi order_item_id (ORDER BY updated_at DESC).
-- Ví dụ dùng app.order_items (không có updated_at) — giả lập: dùng created_at.

WITH staged AS (
  SELECT order_item_id, order_id, product_id, quantity, subtotal, created_at
  FROM app.order_items
),
deduped AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY created_at DESC) AS rn
  FROM staged
)
SELECT order_item_id, order_id, product_id, quantity, subtotal, created_at
FROM deduped
WHERE rn = 1;

-- ----------------------------------------------------------------------------
-- CASE 7: ETL-style — "Incremental": chỉ orders sau ngày X (vd: last_load_date)
-- ----------------------------------------------------------------------------
-- Đề: Chỉ lấy order_items thuộc orders có order_date sau một ngày cố định (incremental load).
--     Trong ETL thực tế: last_load_date lấy từ bảng control hoặc parameter.
--     Ở đây dùng CURRENT_DATE - 7 ngày làm ví dụ (chỉ lấy đơn 7 ngày qua).

SELECT oi.order_item_id, oi.order_id, o.order_date, oi.product_id, oi.quantity, oi.subtotal
FROM app.order_items oi
JOIN app.orders o ON o.order_id = oi.order_id
WHERE o.order_date > CURRENT_DATE - INTERVAL '7 days'
ORDER BY o.order_date, oi.order_item_id;
