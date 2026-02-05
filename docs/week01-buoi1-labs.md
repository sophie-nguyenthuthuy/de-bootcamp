# Week 1 · Buổi 1 — Lab & Demo Guide

**Big Data Engineer Overview · OLTP · SQL Foundations**

---

## Nội dung buổi học (tóm tắt)

- Vai trò Data Engineer trong hệ Big Data  
- **OLTP vs OLAP**, ACID  
- Kiến trúc **DW**, **Data Lake**, **Lakehouse**  
- **Lambda vs Kappa**  
- **Lab 1:** Docker Compose cài PostgreSQL  
- **Lab 2:** Load sample dataset (retail / e‑commerce)

---

## Chuẩn bị (trước buổi học)

- Docker Desktop đã cài và chạy được.
- Repo đã clone: `git clone ... && cd de-bootcamp`.

---

# Lab 1: Docker Compose cài PostgreSQL

**Mục tiêu:** Chạy PostgreSQL trong container, hiểu OLTP “app database” trong kiến trúc sau này (DW / Lakehouse).

### Bước 1 — Chỉ khởi động Postgres

```bash
cd de-bootcamp
docker compose up -d postgres
```

### Bước 2 — Kiểm tra container và port

```bash
docker ps
```

Cần thấy container **de_postgres** (hoặc `de-bootcamp-postgres-1`) đang **Up**, port **5432**.

### Bước 3 — Kết nối bằng `psql`

```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db
```

Hoặc:

```bash
psql -h localhost -p 5432 -U de_user -d de_db
# Password: de_pass
```

### Bước 4 — Thử vài lệnh SQL (OLTP “transactional”)

```sql
\dt
SELECT current_database(), current_user;
\q
```

**Giải thích demo:** Đây là **OLTP** — database phục vụ app (transactions, ACID). Sau này sẽ dùng làm nguồn cho DW/Lakehouse (ETL, batch, streaming).

---

# Lab 2: Load sample dataset (retail / e‑commerce)

**Mục tiêu:** Tạo schema OLTP (customers, products, orders, order_items) và load dữ liệu mẫu bán hàng.

### Bước 1 — Đảm bảo Postgres đang chạy

```bash
docker compose up -d postgres
```

### Bước 2 — Tạo schema và bảng (OLTP)

```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/01_create_oltp_schema.sql
```

**Giải thích demo:**

- Schema `app` = “application” (OLTP).
- Bảng: **customers**, **products**, **orders**, **order_items** — mô hình retail/e‑commerce điển hình.
- Trigger: tự cập nhật `total_amount` khi thêm/sửa `order_items` (ACID, consistency).

### Bước 3 — Load dữ liệu mẫu

```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/02_seed_sample_data.sql
```

**Giải thích demo:** 5 khách hàng, 6 sản phẩm, 5 đơn hàng + order items — đủ để làm ví dụ SQL và sau này ETL sang Lakehouse.

### Bước 4 — Kiểm tra dữ liệu (SQL Foundations)

Kết nối lại và chạy:

```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db
```

```sql
-- Liệt kê bảng trong schema app
SET search_path TO app;
\dt

-- Xem customers
SELECT * FROM app.customers;

-- Xem products
SELECT * FROM app.products;

-- Orders với tổng tiền
SELECT order_id, customer_id, order_date, total_amount, status
FROM app.orders
ORDER BY order_date DESC;

-- JOIN: đơn hàng + khách hàng
SELECT o.order_id, c.full_name, o.order_date, o.total_amount, o.status
FROM app.orders o
JOIN app.customers c ON c.customer_id = o.customer_id
ORDER BY o.order_date DESC;

-- Doanh thu theo ngày (sẹt cho OLAP/DW sau này)
SELECT order_date, COUNT(*) AS num_orders, SUM(total_amount) AS revenue
FROM app.orders
WHERE status = 'completed'
GROUP BY order_date
ORDER BY order_date;
```

### Bước 5 — (Tuỳ chọn) Dùng Makefile

```bash
make init-db
```

Lệnh này chạy cả `01_create_oltp_schema.sql` và `02_seed_sample_data.sql`. Dùng khi muốn reset và load lại từ đầu.

---

# Demo flow gợi ý (cho instructor)

1. **Mở đầu (5 phút)**  
   - Nhắc lại: Data Engineer, OLTP vs OLAP, DW / Lake / Lakehouse, Lambda vs Kappa (slide hoặc bảng).

2. **Lab 1 — Postgres bằng Docker (10–15 phút)**  
   - Chạy `docker compose up -d postgres` → `docker ps` → `psql` → vài lệnh SQL đơn giản.  
   - Nhấn mạnh: đây là “app DB” (OLTP), sẽ là nguồn cho ETL/batch/stream sau này.

3. **Lab 2 — Schema + data retail (15–20 phút)**  
   - Chạy `01_create_oltp_schema.sql` → giải thích bảng và trigger.  
   - Chạy `02_seed_sample_data.sql` → vài `SELECT` / `JOIN` / `GROUP BY` như trên.  
   - Gợi ý: các câu “doanh thu theo ngày” chính là kiểu query OLAP/DW — sau này sẽ đưa vào Lakehouse/Trino.

4. **Kết (5 phút)**  
   - Tóm tắt: đã có OLTP + sample data; buổi sau sẽ dùng MinIO/Trino, ETL, v.v.

---

# Checklist học viên (sau buổi)

- [ ] Postgres chạy bằng `docker compose` và kết nối được bằng `psql`.
- [ ] Schema `app` với 4 bảng (customers, products, orders, order_items) đã tạo và có data.
- [ ] Chạy được ít nhất 2–3 câu SQL: `SELECT`, `JOIN`, `GROUP BY` (như ví dụ trên).

---

# Tham chiếu nhanh

| Mục            | Giá trị                          |
|----------------|-----------------------------------|
| Host           | `localhost`                       |
| Port           | `5432`                            |
| Database       | `de_db`                           |
| User           | `de_user`                         |
| Password       | `de_pass`                         |
| Connection URI | `postgresql://de_user:de_pass@localhost:5432/de_db` |
| Schema OLTP   | `app`                             |
