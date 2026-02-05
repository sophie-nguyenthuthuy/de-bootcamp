# Week 4 · Buổi 7 — ETL/ELT & Data Warehouse

**Kimball vs Inmon · ETL vs ELT · Slowly Changing Dimensions · PostgreSQL + Metabase · Build dashboard đầu tiên**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Lý thuyết | **Kimball vs Inmon** — bottom-up (data mart) vs top-down (EDW) |
| Lý thuyết | **ETL vs ELT** — transform trước/sau load, khi nào dùng |
| Lý thuyết | **Slowly Changing Dimensions** — SCD1, SCD2, SCD6 (ôn Buổi 3) |
| Lab 1 | **Docker Compose: PostgreSQL + Metabase** — chạy stack, mở Metabase UI |
| Lab 2 | **Build dashboard đầu tiên** — kết nối Postgres, tạo Question, tạo Dashboard |

---

## Chuẩn bị

- Docker đang chạy.
- Schema `app` và data đã có (Buổi 1); (tuỳ chọn) data mart `mart` (Buổi 3).

```bash
docker compose up -d postgres
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/01_create_oltp_schema.sql
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/02_seed_sample_data.sql
```

---

## Lý thuyết tóm tắt

### Kimball vs Inmon

| | Kimball | Inmon |
|---|---------|--------|
| **Hướng** | Bottom-up: data mart → DW | Top-down: EDW → data mart |
| **Mô hình** | Star schema, dimension + fact | 3NF / normalized EDW trước |
| **Ưu** | Triển khai nhanh, query đơn giản | Một nguồn sự thật, ít trùng lặp |
| **Nhược** | Có thể trùng logic giữa mart | Chi phí, thời gian xây EDW lớn |

### ETL vs ELT

| | ETL | ELT |
|---|-----|-----|
| **Transform** | Trước khi load (staging → transform → DW) | Sau khi load (raw vào DW/lake → SQL/spark transform) |
| **Khi nào** | DW truyền thống, nguồn nhỏ | Lakehouse, Big Data, dùng engine (Trino/Spark) transform |

### Slowly Changing Dimensions (ôn)

- **SCD1:** Ghi đè — không lưu lịch sử.
- **SCD2:** Thêm row mới — valid_from, valid_to, is_current.
- **SCD6:** Hybrid — một số cột SCD1, một số SCD2.

---

## Lab 1: Docker Compose — PostgreSQL + Metabase

**Mục tiêu:** Chạy Postgres và Metabase bằng Docker Compose, mở Metabase UI.

### Bước 1 — Khởi động Postgres + Metabase

```bash
cd de-bootcamp
docker compose up -d postgres metabase
```

Đợi Metabase khởi động (lần đầu có thể 1–2 phút).

### Bước 2 — Mở Metabase UI

Trình duyệt: **http://localhost:3000**

- Lần đầu: Metabase yêu cầu thiết lập admin (email, mật khẩu) — điền và tạo account.
- Sau đó vào giao diện chính.

### Bước 3 — Kiểm tra Postgres đang chạy

```bash
docker compose ps
```

Cần thấy **de_postgres** và **de_metabase** (hoặc de-bootcamp-postgres-1, de-bootcamp-metabase-1) đang **Up**.

---

## Lab 2: Build dashboard đầu tiên

**Mục tiêu:** Kết nối Metabase với Postgres (`de_db`), tạo Question (query), tạo Dashboard và gắn Question vào.

### Bước 1 — Thêm database (Data source)

1. Trong Metabase: **Settings** (bánh răng) → **Admin settings** → **Databases** (hoặc **Add database**).
2. **Add database:**
   - **Database type:** PostgreSQL
   - **Display name:** de_db (hoặc "App DB")
   - **Host:** `postgres` (tên service trong Docker; nếu Metabase chạy ngoài Docker dùng `localhost`)
   - **Port:** `5432`
   - **Database name:** `de_db`
   - **Username:** `de_user`
   - **Password:** `de_pass`
3. **Save**; chờ Metabase sync schema (Sync database now).

**Lưu ý:** Nếu Metabase chạy trên host (không trong Docker) mà Postgres trong Docker, dùng Host: `localhost`, Port: `5432`.

### Bước 2 — Tạo Question (câu hỏi / query)

1. **New** → **Question** → **Native query** (hoặc **Simple question** nếu thích dùng GUI).
2. Chọn database **de_db**.
3. Ví dụ **Native query:**

```sql
SELECT order_date, COUNT(*) AS num_orders, SUM(total_amount) AS revenue
FROM app.orders
WHERE status = 'completed'
GROUP BY order_date
ORDER BY order_date DESC;
```

4. **Run** → đặt tên Question (vd: "Revenue by order date") → **Save**.

### Bước 3 — Tạo Dashboard và gắn Question

1. **New** → **Dashboard** → đặt tên (vd: "Sales Overview").
2. **Add a saved question** → chọn Question vừa tạo (vd: "Revenue by order date").
3. Chọn kiểu biểu đồ (vd: Table, Bar chart, Line chart) và tuỳ chỉnh.
4. **Save** dashboard.

### Bước 4 — Thêm vài Question khác (tuỳ chọn)

- **Top customers by total spent:**  
  `SELECT c.full_name, SUM(o.total_amount) AS total_spent FROM app.orders o JOIN app.customers c ON c.customer_id = o.customer_id GROUP BY c.customer_id, c.full_name ORDER BY total_spent DESC LIMIT 10;`
- **Revenue by product category (từ order_items + products):**  
  JOIN `app.order_items`, `app.products`, `app.orders`; group by category; sum(subtotal).

Lưu từng câu hỏi rồi thêm vào Dashboard.

### Bước 5 — (Tuỳ chọn) Dùng data mart `mart`

Nếu đã có schema **mart** (Buổi 3): thêm Question từ `mart.fact_sales`, `mart.dim_customer`, `mart.dim_product`, `mart.dim_date` — ví dụ doanh thu theo tháng, theo khách — và gắn vào Dashboard.

---

## Demo flow gợi ý (instructor)

1. **Lý thuyết (20–25 phút):** Kimball vs Inmon, ETL vs ELT, SCD (1/2/6) — slide + bảng so sánh.
2. **Lab 1 — Postgres + Metabase (15–20 phút):** `docker compose up -d postgres metabase`; mở http://localhost:3000; thiết lập admin; kiểm tra Postgres và Metabase đang chạy.
3. **Lab 2 — Dashboard (30–40 phút):** Thêm database de_db; tạo 1–2 Native query (revenue by date, top customers); tạo Dashboard; thêm Question vào Dashboard; chọn chart type.
4. **Kết (5 phút):** Tóm tắt: Kimball/Inmon, ETL/ELT; Metabase = BI trên DW/DB; dashboard = nhiều Question trên một màn hình.

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Metabase UI | http://localhost:3000 |
| Postgres (trong Docker) | host=`postgres`, port=5432, db=`de_db`, user=`de_user`, pass=`de_pass` |
| Postgres (từ host) | host=`localhost`, port=5432, db=`de_db` |
| Schema OLTP | `app` (orders, customers, products, order_items) |
| Schema Mart (tuỳ chọn) | `mart` (fact_sales, dim_customer, dim_product, dim_date) |
| Lệnh khởi động | `docker compose up -d postgres metabase` |
