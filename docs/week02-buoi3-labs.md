# Week 2 · Buổi 3 — Data Modeling

**Star Schema, Snowflake · SCD (1, 2, 6) · Data Vault 2.0 overview · Data mart · Dimension + Fact + SCD2 bằng SQL**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Lý thuyết | **Star Schema** vs **Snowflake** (dimension, fact, grain) |
| Lý thuyết | **SCD:** Type 1 (overwrite), Type 2 (history + valid_from/to), Type 6 (hybrid) |
| Lý thuyết | **Data Vault 2.0** overview (Hub, Link, Satellite) |
| Lab 1 | **Model 1 data mart** — Sales mart từ OLTP `app` (star schema) |
| Lab 2 | **Thiết kế bảng dimension + fact + SCD2 bằng SQL** (DDL + ETL mẫu) |

---

## Chuẩn bị

- Postgres đang chạy; schema `app` và data đã load (Buổi 1):

```bash
docker compose up -d postgres
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/01_create_oltp_schema.sql
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/02_seed_sample_data.sql
```

- File lab:
  - **`sql/week02_buoi3_datamart_ddl.sql`** — DDL: dimension, fact, SCD2
  - **`sql/week02_buoi3_datamart_etl.sql`** — ETL: populate mart từ `app`

---

## Lý thuyết tóm tắt

### Star Schema vs Snowflake

| | Star Schema | Snowflake |
|---|-------------|-----------|
| Dimension | Bảng dimension phẳng, ít chuẩn hóa | Dimension có thể chuẩn hóa (vd: category tách bảng) |
| JOIN | Fact JOIN trực tiếp nhiều dim | Fact → dim → sub-dim |
| Độ phức tạp | Đơn giản, query nhanh | Phức tạp hơn, giảm redundancy |

**Data mart trong lab:** Star schema — 1 fact (fact_sales), 3 dimension (dim_date, dim_customer, dim_product).

### SCD (Slowly Changing Dimension)

| Type | Mô tả | Ví dụ |
|------|--------|--------|
| **SCD1** | Ghi đè; không lưu lịch sử | Cập nhật email → row cũ mất |
| **SCD2** | Thêm row mới; valid_from, valid_to, is_current | Mỗi lần thay đổi = 1 row mới, row cũ đóng (valid_to) |
| **SCD6** | Hybrid: một số cột SCD1, một số SCD2 | Current name (SCD1) + history name (SCD2) |

**Lab:** Thiết kế **dim_customer** và **dim_product** theo **SCD2** (surrogate key, valid_from, valid_to, is_current).

### Data Vault 2.0 (overview)

- **Hub:** Business key (vd: customer_id, product_id) — 1 hub = 1 loại entity.
- **Link:** Quan hệ nhiều-nhiều giữa hub (vd: order–product).
- **Satellite:** Thuộc tính theo thời gian (gắn Hub hoặc Link); lịch sử = nhiều satellite record.

Không bắt buộc implement trong lab; chỉ cần nắm concept để so sánh với Star/SCD2.

---

## Lab 1: Model 1 data mart

**Mục tiêu:** Thiết kế 1 data mart **Sales** (star schema) từ OLTP `app`.

### Mô hình (Star)

- **Fact:** `fact_sales` — grain = 1 dòng / 1 order_item (order_item_id).
  - Measure: quantity, amount (subtotal).
  - Foreign key: date_key, customer_sk, product_sk.
- **Dimensions:**
  - **dim_date:** date_key, date_actual, year, month, day_of_week (dùng cho phân tích theo thời gian).
  - **dim_customer:** SCD2 — customer_sk, customer_id (natural key), full_name, email, phone_number, valid_from, valid_to, is_current.
  - **dim_product:** SCD2 — product_sk, product_id, product_name, category, price, valid_from, valid_to, is_current.

### Bước thực hiện

1. Tạo schema `mart` và các bảng (DDL): chạy **`sql/week02_buoi3_datamart_ddl.sql`**.
2. Populate mart từ `app`: chạy **`sql/week02_buoi3_datamart_etl.sql`**.
3. Kiểm tra: query fact + dimensions, thử vài câu phân tích (doanh thu theo tháng, theo khách, theo sản phẩm).

**Ví dụ query phân tích (sau khi load mart):**

```sql
-- Doanh thu theo tháng
SELECT d.year, d.month, SUM(f.amount) AS revenue, COUNT(*) AS num_items
FROM mart.fact_sales f
JOIN mart.dim_date d ON d.date_key = f.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Doanh thu theo khách (dim_customer)
SELECT c.full_name, SUM(f.amount) AS total_spent, SUM(f.quantity) AS total_qty
FROM mart.fact_sales f
JOIN mart.dim_customer c ON c.customer_sk = f.customer_sk
WHERE c.is_current = TRUE
GROUP BY c.customer_sk, c.full_name
ORDER BY total_spent DESC;

-- Doanh thu theo sản phẩm (dim_product)
SELECT p.product_name, p.category, SUM(f.amount) AS revenue, SUM(f.quantity) AS qty_sold
FROM mart.fact_sales f
JOIN mart.dim_product p ON p.product_sk = f.product_sk
WHERE p.is_current = TRUE
GROUP BY p.product_sk, p.product_name, p.category
ORDER BY revenue DESC;
```

```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/week02_buoi3_datamart_ddl.sql
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/week02_buoi3_datamart_etl.sql
```

---

## Lab 2: Thiết kế bảng dimension + fact + SCD2 bằng SQL

**Mục tiêu:** Tự thiết kế (hoặc đọc và chỉnh sửa) DDL + ETL cho dimension (SCD2), fact.

### Nội dung trong file

- **week02_buoi3_datamart_ddl.sql**
  - `mart.dim_date`: date_key (PK), date_actual, year, month, day_of_week.
  - `mart.dim_customer`: customer_sk (PK), customer_id, full_name, email, phone_number, valid_from, valid_to, is_current.
  - `mart.dim_product`: product_sk (PK), product_id, product_name, category, price, valid_from, valid_to, is_current.
  - `mart.fact_sales`: order_item_id (grain), order_id, date_key, customer_sk, product_sk, quantity, amount.

- **week02_buoi3_datamart_etl.sql**
  - Populate `dim_date` (vd: vài năm xung quanh order_date trong `app`).
  - Populate `dim_customer` SCD2: “current” row cho mỗi customer (valid_to = '9999-12-31', is_current = true).
  - Populate `dim_product` SCD2: tương tự.
  - Populate `fact_sales`: JOIN app.orders, order_items, products, customers → lấy date_key, customer_sk, product_sk (theo is_current = true) và quantity, amount.

### Câu hỏi / bài tập

1. Tại sao fact dùng `customer_sk` / `product_sk` thay vì `customer_id` / `product_id`?
2. Khi nào cần “mở” row mới trong SCD2? (khi attribute thay đổi)
3. Query “doanh thu theo tháng” dùng dim_date như thế nào?

---

## Demo flow gợi ý (instructor)

1. **Lý thuyết (25–30 phút):** Star vs Snowflake, SCD1/2/6 (bảng, ví dụ), Data Vault 2.0 (Hub/Link/Satellite).
2. **Lab 1 — Model data mart (20–25 phút):** Vẽ star schema trên bảng/trình chiếu; chạy DDL + ETL; chạy 1–2 query phân tích.
3. **Lab 2 — Dimension + Fact + SCD2 (25–30 phút):** Đi qua từng bảng trong DDL (dim_date, dim_customer SCD2, dim_product SCD2, fact_sales); giải thích ETL SCD2 (valid_from, valid_to, is_current); học viên tự chạy file và thử đổi 1 record trong `app` rồi “refresh” 1 dimension (nếu có thời gian).
4. **Kết (5 phút):** Tóm tắt: grain của fact, vai trò SCD2, khi nào dùng Star vs Snowflake vs Data Vault.

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Connection | `postgresql://de_user:de_pass@localhost:5432/de_db` |
| Schema OLTP | `app` (customers, products, orders, order_items) |
| Schema Mart | `mart` (dim_date, dim_customer, dim_product, fact_sales) |
| DDL | `sql/week02_buoi3_datamart_ddl.sql` |
| ETL | `sql/week02_buoi3_datamart_etl.sql` |
