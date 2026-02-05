# Week 2 · Buổi 4 — Ôn SQL + Mini Case Study Data Mart

**Review SQL nâng cao (window, CTE, performance) · Best practices SQL trong ETL · Hoàn thiện Data Mart · Bài tập SQL business case thực tế**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Ôn tập | **SQL nâng cao:** Window functions, CTE, performance (index, EXPLAIN) |
| Lý thuyết | **Best practices** viết SQL trong ETL (idempotent, incremental, dedup) |
| Lab 1 | **Hoàn thiện Data Mart** (tuần 1 / Buổi 3): kiểm tra, bổ sung, chạy lại ETL |
| Lab 2 | **Bài tập SQL business case thực tế** (file `sql/week02_buoi4_business_case.sql`) |

---

## Chuẩn bị

- Postgres đang chạy; schema **`app`** và **`mart`** đã có (Buổi 1 + Buổi 3):

```bash
docker compose up -d postgres
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/01_create_oltp_schema.sql
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/02_seed_sample_data.sql
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/week02_buoi3_datamart_ddl.sql
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/week02_buoi3_datamart_etl.sql
```

- File lab: **`sql/week02_buoi4_business_case.sql`** — bài tập business case (đề + solution).

---

## Ôn SQL nâng cao (tóm tắt)

### Window functions

- **ROW_NUMBER(), RANK(), DENSE_RANK()** — xếp hạng (partition, order).
- **SUM/AVG OVER (PARTITION BY ... ORDER BY ...)** — tổng/trung bình tích lũy hoặc theo nhóm.
- **LAG(), LEAD()** — so sánh với dòng trước/sau (vd: MoM, YoY).

Ví dụ: Top 3 khách theo doanh thu, running total doanh thu theo ngày, % đóng góp category.

### CTE (Common Table Expression)

- **WITH cte AS (SELECT ...)** — đặt tên cho subquery, dễ đọc, tái sử dụng.
- Dùng cho: tổng hợp theo nhóm rồi filter (vd: revenue per customer → HAVING), xếp hạng rồi lấy top N, chuẩn bị dữ liệu cho bước ETL tiếp.

### Performance

- **EXPLAIN (ANALYZE, BUFFERS)** — đọc plan: Seq Scan vs Index Scan, cost, actual time.
- **Index** — cột trong WHERE, JOIN, ORDER BY; trade-off với INSERT/UPDATE.
- Viết lại query: subquery → JOIN, tránh SELECT * khi không cần.

---

## Best practices SQL trong ETL

| Practice | Mô tả |
|----------|--------|
| **Idempotent** | Chạy nhiều lần cùng input → cùng output; dùng MERGE/ON CONFLICT/INSERT WHERE NOT EXISTS. |
| **Incremental** | Chỉ xử lý dữ liệu mới/thay đổi (vd: WHERE order_date > last_load_date). |
| **Dedup** | Trước khi load fact/dim: ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY updated_at DESC) rồi lấy row_number = 1. |
| **Set-based** | Tránh vòng lặp row-by-row; dùng INSERT/SELECT, UPDATE FROM. |
| **Transaction** | Gói ETL trong BEGIN/COMMIT; lỗi thì ROLLBACK. |
| **Naming** | Đặt tên rõ ràng (stg_, dim_, fact_), comment cho bảng/cột phức tạp. |

Trong lab: ETL mart dùng ON CONFLICT DO NOTHING (idempotent); business case có bài “incremental” và “dedup” bằng window.

---

## Lab 1: Hoàn thiện Data Mart (tuần 1)

**Mục tiêu:** Đảm bảo Data Mart Sales (Buổi 3) đầy đủ, chạy đúng, và có vài kiểm tra chất lượng.

### Bước 1 — Kiểm tra schema và data

```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db
```

```sql
-- Số dòng từng bảng mart
SELECT 'dim_date' AS tbl, COUNT(*) AS cnt FROM mart.dim_date
UNION ALL SELECT 'dim_customer', COUNT(*) FROM mart.dim_customer
UNION ALL SELECT 'dim_product', COUNT(*) FROM mart.dim_product
UNION ALL SELECT 'fact_sales', COUNT(*) FROM mart.fact_sales;

-- Fact phải join được với cả 3 dim (không orphan)
SELECT COUNT(*) AS fact_rows,
       COUNT(d.date_key) AS with_date,
       COUNT(c.customer_sk) AS with_customer,
       COUNT(p.product_sk) AS with_product
FROM mart.fact_sales f
LEFT JOIN mart.dim_date d ON d.date_key = f.date_key
LEFT JOIN mart.dim_customer c ON c.customer_sk = f.customer_sk
LEFT JOIN mart.dim_product p ON p.product_sk = f.product_sk;
-- fact_rows = with_date = with_customer = with_product (nếu thiết kế đúng)
```

### Bước 2 — Chạy lại ETL (idempotent)

Sau khi thêm/sửa data trong `app` (vd: thêm order mới, hoặc cập nhật customer):

```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/week02_buoi3_datamart_etl.sql
```

Kiểm tra lại: fact_sales tăng đúng số dòng mới (nếu có order_items mới).

### Bước 3 — Bổ sung (tuỳ chọn)

- Thêm 1–2 câu query phân tích vào tài liệu Buổi 3 (vd: doanh thu theo quarter, top 5 sản phẩm).
- Ghi chú: với SCD2, khi nào cần “đóng” row cũ và “mở” row mới (khi attribute trong source thay đổi).

---

## Lab 2: Bài tập SQL business case thực tế

**Mục tiêu:** Viết SQL giải quyết bài toán nghiệp vụ (doanh thu, xếp hạng, growth, ETL-style).

File: **`sql/week02_buoi4_business_case.sql`**

Nội dung gồm:

- **Case 1–2:** Data Mart — Top N khách theo doanh thu (RANK), running total doanh thu theo ngày (window).
- **Case 3–4:** CTE — % đóng góp doanh thu theo category; so sánh doanh thu theo tháng (LAG / self-join).
- **Case 5:** Khách mua trong khoảng thời gian A nhưng không mua trong B (NOT EXISTS / LEFT JOIN).
- **Case 6–7:** ETL-style — Dedup bằng ROW_NUMBER; “incremental” (chỉ orders sau ngày X).

Tự viết SQL trước, sau đó so với solution trong file. Chạy trên `de_db`, schema `app` và `mart`.

```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/week02_buoi4_business_case.sql
```

Hoặc mở file, copy từng block (đề + solution) vào psql.

---

## Demo flow gợi ý (instructor)

1. **Ôn SQL (15–20 phút):** Window (RANK, SUM OVER), CTE, EXPLAIN; ví dụ 1–2 câu trên mart.
2. **Best practices ETL (10 phút):** Idempotent, incremental, dedup; liên hệ với ETL mart đã viết (ON CONFLICT).
3. **Lab 1 — Hoàn thiện mart (15–20 phút):** Chạy kiểm tra số dòng, fact–dim integrity; chạy lại ETL; (tuỳ chọn) thêm 1 query phân tích.
4. **Lab 2 — Business case (30–40 phút):** Học viên làm từng case; instructor review 2–3 bài (vd: top N, running total, dedup).
5. **Kết (5 phút):** Tóm tắt: khi nào dùng window vs CTE, best practices ETL, cách đọc business case và map sang SQL.

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Connection | `postgresql://de_user:de_pass@localhost:5432/de_db` |
| Schema OLTP | `app` |
| Schema Mart | `mart` |
| Business case SQL | `sql/week02_buoi4_business_case.sql` |
| Mart DDL/ETL | `sql/week02_buoi3_datamart_ddl.sql`, `sql/week02_buoi3_datamart_etl.sql` |
