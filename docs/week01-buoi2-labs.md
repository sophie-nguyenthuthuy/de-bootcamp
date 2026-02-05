# Week 1 · Buổi 2 — SQL Cơ Bản → Nâng Cao

**SQL joins, subquery, CTE · Window functions · Index, partitioning · 25 SQL challenges · Query optimization**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Lý thuyết | SQL JOINs (INNER, LEFT, nhiều bảng), subquery, CTE |
| Lý thuyết | Window functions (ROW_NUMBER, RANK, SUM OVER, PARTITION BY) |
| Lý thuyết | Index, partitioning (khi nào dùng, ảnh hưởng query) |
| Lab 1 | **25 bài SQL challenge** (schema `app`: customers, products, orders, order_items) |
| Lab 2 | **Query optimization** (EXPLAIN ANALYZE, index, rewrite query) |

---

## Chuẩn bị

- Postgres đang chạy (`docker compose up -d postgres`).
- Schema và data đã load (Buổi 1):

```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/01_create_oltp_schema.sql
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/02_seed_sample_data.sql
```

- File challenge: **`sql/week01_buoi2_challenges.sql`** (25 bài có đề + solution).

---

## Lab 1: 25 bài SQL challenge

Mở **`sql/week01_buoi2_challenges.sql`** và làm lần lượt. Tóm tắt theo chủ đề:

| # | Chủ đề | Mô tả ngắn |
|---|--------|------------|
| 1–5 | Cơ bản | SELECT, WHERE, ORDER BY, LIMIT, EXISTS/IN |
| 6–10 | JOINs | INNER, LEFT, nhiều bảng, order count, “no match” |
| 11–13 | Subquery | So sánh với AVG, SUM, MAX trong subquery |
| 14–16 | CTE | Revenue per customer, top N per category, order item count |
| 17–20 | Window | ROW_NUMBER, RANK, running total, SUM OVER (PARTITION BY) |
| 21–23 | Index & partitioning | Tạo index, EXPLAIN, khái niệm partitioning |
| 24–25 | Optimization | Viết lại query, EXPLAIN ANALYZE, gợi ý index |

**Cách làm:** Tự viết câu SQL trước, sau đó so với solution trong file. Chạy trên `de_db`, schema `app`.

```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db
```

```sql
SET search_path TO app;
-- paste từng challenge/solution từ week01_buoi2_challenges.sql
```

---

## Lab 2: Query optimization

### 2.1 EXPLAIN và EXPLAIN ANALYZE

- **EXPLAIN:** Kế hoạch thực thi (cost, seq scan vs index scan).
- **EXPLAIN ANALYZE:** Thực thi thật và trả về thời gian + rows.

Ví dụ:

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT o.order_id, c.full_name, o.total_amount
FROM app.orders o
JOIN app.customers c ON c.customer_id = o.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days';
```

Quan sát: **Seq Scan** vs **Index Scan**, cost, actual time, rows.

### 2.2 Thêm index và so sánh

Trước index:

```sql
EXPLAIN ANALYZE
SELECT * FROM app.orders WHERE order_date = CURRENT_DATE - 1;
```

Tạo index:

```sql
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON app.orders(order_date);
```

Chạy lại EXPLAIN ANALYZE → so sánh plan và thời gian.

### 2.3 Viết lại query (subquery → JOIN)

Ví dụ “customers with at least one order”:

- Cách 1 (subquery): `WHERE customer_id IN (SELECT customer_id FROM app.orders)`
- Cách 2 (JOIN): `FROM app.customers c JOIN app.orders o ON ...` hoặc `EXISTS`

Chạy EXPLAIN ANALYZE cho cả hai, so sánh cost và thời gian (với data nhỏ có thể gần nhau; với data lớn JOIN/EXISTS thường ổn hơn IN (subquery) nếu không được tối ưu).

### 2.4 Checklist optimization

- [ ] Đọc được output EXPLAIN (ANALYZE, BUFFERS).
- [ ] Tạo index phù hợp (cột trong WHERE/JOIN) và thấy Index Scan.
- [ ] Thử viết lại 1 query (subquery ↔ JOIN) và so sánh plan.

---

## Index và partitioning (tóm tắt lý thuyết)

### Index

- **Mục đích:** Giảm full table scan khi filter/sort/join theo cột đã index.
- **Khi nào:** Cột hay dùng trong `WHERE`, `ORDER BY`, `JOIN`.
- **Trade-off:** Tốn dung lượng và chậm hơn khi INSERT/UPDATE/DELETE.

Ví dụ với schema `app`:

```sql
CREATE INDEX idx_orders_customer_date ON app.orders(customer_id, order_date);
CREATE INDEX idx_order_items_order_id ON app.order_items(order_id);
```

### Partitioning

- **Mục đích:** Chia bảng lớn theo range/list (vd: theo `order_date` theo tháng) để query chỉ scan partition cần thiết.
- **Khi nào:** Bảng rất lớn, query thường filter theo cột partition (vd: ngày).
- **Postgres:** Dùng partitioned table (PARTITION BY RANGE / LIST); bảng hiện tại `app.orders` có thể giữ nguyên, khi dạy có thể tạo bảng mới partitioned để demo concept.

---

## Demo flow gợi ý (instructor)

1. **Lý thuyết (20–25 phút):** JOINs, subquery, CTE, window functions, index/partitioning (slide + ví dụ 1–2 câu SQL).
2. **Lab 1 — 25 challenges (45–60 phút):** Học viên làm từng nhóm (1–5, 6–10, …); instructor review solution vài bài tiêu biểu (JOIN, CTE, window).
3. **Lab 2 — Optimization (20–25 phút):** Demo EXPLAIN ANALYZE trước/sau index; demo viết lại 1 query; học viên tự thử với 1–2 query.
4. **Kết (5 phút):** Tóm tắt: index khi nào, partitioning khi nào, cách đọc EXPLAIN.

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Connection | `postgresql://de_user:de_pass@localhost:5432/de_db` |
| Schema | `app` |
| Bảng | `customers`, `products`, `orders`, `order_items` |
| File challenges | `sql/week01_buoi2_challenges.sql` |
