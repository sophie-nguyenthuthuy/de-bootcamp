-- ============================================================================
-- WEEK 01 · BUỔI 2 — 25 SQL CHALLENGES
-- Schema: app (customers, products, orders, order_items)
-- Chạy: psql ... -f sql/week01_buoi2_challenges.sql
-- Hoặc copy từng block vào psql. SET search_path TO app; trước khi chạy.
-- ============================================================================
SET search_path TO app;

-- ----------------------------------------------------------------------------
-- 1–5: CƠ BẢN — SELECT, WHERE, ORDER BY, LIMIT
-- ----------------------------------------------------------------------------

-- Challenge 1: Liệt kê toàn bộ khách hàng (full_name, email).
SELECT full_name, email FROM customers;

-- Challenge 2: Sản phẩm thuộc category 'Electronics', sắp xếp giá giảm dần.
SELECT * FROM products WHERE category = 'Electronics' ORDER BY price DESC;

-- Challenge 3: Đơn hàng có total_amount > 20,000,000.
SELECT * FROM orders WHERE total_amount > 20000000;

-- Challenge 4: Top 3 sản phẩm có giá cao nhất.
SELECT * FROM products ORDER BY price DESC LIMIT 3;

-- Challenge 5: Khách hàng đã có ít nhất một đơn hàng (dùng EXISTS).
SELECT * FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id);

-- ----------------------------------------------------------------------------
-- 6–10: JOINS — INNER, LEFT, nhiều bảng
-- ----------------------------------------------------------------------------

-- Challenge 6: Mỗi đơn: order_id, tên khách, order_date, total_amount.
SELECT o.order_id, c.full_name, o.order_date, o.total_amount
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id;

-- Challenge 7: Order items kèm tên sản phẩm và category.
SELECT oi.order_item_id, oi.order_id, p.product_name, p.category, oi.quantity, oi.subtotal
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id;

-- Challenge 8: Chi tiết đơn: order_id, tên khách, tên SP, quantity, subtotal.
SELECT o.order_id, c.full_name, p.product_name, oi.quantity, oi.subtotal
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
JOIN order_items oi ON oi.order_id = o.order_id
JOIN products p ON p.product_id = oi.product_id
ORDER BY o.order_id, oi.order_item_id;

-- Challenge 9: Mỗi khách và số đơn đã đặt (kể cả 0 đơn).
SELECT c.customer_id, c.full_name, COUNT(o.order_id) AS order_count
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.full_name;

-- Challenge 10: Đơn hàng không có order_items (LEFT JOIN + NULL).
SELECT o.*
FROM orders o
LEFT JOIN order_items oi ON oi.order_id = o.order_id
WHERE oi.order_item_id IS NULL;

-- ----------------------------------------------------------------------------
-- 11–13: SUBQUERY
-- ----------------------------------------------------------------------------

-- Challenge 11: Sản phẩm có giá cao hơn giá trung bình.
SELECT * FROM products
WHERE price > (SELECT AVG(price) FROM products);

-- Challenge 12: Khách hàng có tổng tiền mua > 25,000,000.
SELECT c.customer_id, c.full_name, SUM(o.total_amount) AS total_spent
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.full_name
HAVING SUM(o.total_amount) > 25000000;

-- Challenge 13: Đơn hàng có total_amount lớn nhất.
SELECT * FROM orders
WHERE total_amount = (SELECT MAX(total_amount) FROM orders);

-- ----------------------------------------------------------------------------
-- 14–16: CTE (Common Table Expression)
-- ----------------------------------------------------------------------------

-- Challenge 14: CTE = doanh thu theo khách; lấy khách có doanh thu > 20,000,000.
WITH customer_revenue AS (
  SELECT customer_id, SUM(total_amount) AS revenue
  FROM orders
  GROUP BY customer_id
)
SELECT c.customer_id, c.full_name, cr.revenue
FROM customers c
JOIN customer_revenue cr ON cr.customer_id = c.customer_id
WHERE cr.revenue > 20000000;

-- Challenge 15: CTE = xếp hạng giá trong từng category; lấy top 2 mỗi category.
WITH ranked_products AS (
  SELECT product_id, product_name, category, price,
         ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rn
  FROM products
)
SELECT product_id, product_name, category, price, rn
FROM ranked_products
WHERE rn <= 2;

-- Challenge 16: CTE = số dòng order_items mỗi đơn; lấy đơn có > 1 dòng.
WITH order_item_count AS (
  SELECT order_id, COUNT(*) AS cnt
  FROM order_items
  GROUP BY order_id
)
SELECT o.*, oic.cnt
FROM orders o
JOIN order_item_count oic ON oic.order_id = o.order_id
WHERE oic.cnt > 1;

-- ----------------------------------------------------------------------------
-- 17–20: WINDOW FUNCTIONS
-- ----------------------------------------------------------------------------

-- Challenge 17: Đánh số thứ tự đơn theo total_amount giảm dần.
SELECT order_id, customer_id, order_date, total_amount,
       ROW_NUMBER() OVER (ORDER BY total_amount DESC) AS rn
FROM orders;

-- Challenge 18: Xếp hạng giá sản phẩm trong từng category (RANK).
SELECT product_id, product_name, category, price,
       RANK() OVER (PARTITION BY category ORDER BY price DESC) AS price_rank
FROM products;

-- Challenge 19: Running total doanh thu theo order_date (tăng dần ngày).
SELECT order_id, order_date, total_amount,
       SUM(total_amount) OVER (ORDER BY order_date, order_id) AS running_total
FROM orders;

-- Challenge 20: Mỗi đơn: total_amount + tổng tiền của toàn bộ đơn của khách đó.
SELECT order_id, customer_id, order_date, total_amount,
       SUM(total_amount) OVER (PARTITION BY customer_id) AS customer_total_spend
FROM orders;

-- ----------------------------------------------------------------------------
-- 21–23: INDEX & PARTITIONING
-- ----------------------------------------------------------------------------

-- Challenge 21: Tạo index trên order_date; dùng cho filter theo ngày.
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);
-- Ví dụ query dùng index:
-- SELECT * FROM orders WHERE order_date = CURRENT_DATE - 1;

-- Challenge 22: Tạo index trên order_items(order_id) cho JOIN.
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);

-- Challenge 23: (Lý thuyết) Partitioning: bảng lớn chia theo RANGE(order_date).
-- Demo concept — tạo bảng partitioned (optional, có thể bỏ qua nếu chỉ dạy lý thuyết):
/*
CREATE TABLE orders_partitioned (
  LIKE orders INCLUDING ALL
) PARTITION BY RANGE (order_date);

CREATE TABLE orders_2025_01 PARTITION OF orders_partitioned
  FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
-- ...
*/

-- ----------------------------------------------------------------------------
-- 24–25: OPTIMIZATION
-- ----------------------------------------------------------------------------

-- Challenge 24: Viết lại "customers with at least one order" — dùng JOIN thay IN.
-- Cách 1 (IN): SELECT * FROM customers WHERE customer_id IN (SELECT customer_id FROM orders);
-- Cách 2 (JOIN, distinct khách):
SELECT DISTINCT c.*
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id;

-- Challenge 25: EXPLAIN ANALYZE một query phức tạp; đọc plan và gợi ý index.
EXPLAIN (ANALYZE, BUFFERS)
SELECT o.order_id, c.full_name, p.product_name, oi.subtotal
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
JOIN order_items oi ON oi.order_id = o.order_id
JOIN products p ON p.product_id = oi.product_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days';

-- Sau khi chạy: quan sát Seq Scan vs Index Scan; nếu toàn Seq Scan có thể thêm index
-- (vd idx_orders_order_date đã tạo ở Challenge 21).
