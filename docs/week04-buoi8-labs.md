# Week 4 · Buổi 8 — Mini Project ETL + Dashboarding

**Review ETL best practices · Data modeling for dashboard (materialized tables) · ETL pipeline CSV → PostgreSQL → Data mart · Dashboard Metabase nâng cao · Recap**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Ôn tập | **ETL best practices** — idempotent, incremental, dedup, set-based, transaction |
| Lý thuyết | **Data modeling for dashboard** — materialized views / tables, pre-aggregation |
| Lab 1 | **ETL pipeline:** CSV → PostgreSQL (app) → Data mart (mart) |
| Lab 2 | **Dashboard Metabase nâng cao** — filters, nhiều Question, materialized data |
| Recap | Tóm tắt tuần 4 và hành trình ETL → DW → BI |

---

## Chuẩn bị

- Docker: Postgres + Metabase đang chạy (Buổi 7).
- Schema `app` và `mart` đã có (Buổi 1 + Buổi 3); Python ETL (Buổi 5) và script ingest (Buổi 6).

```bash
docker compose up -d postgres metabase
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/01_create_oltp_schema.sql
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/02_seed_sample_data.sql
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/week02_buoi3_datamart_ddl.sql
```

---

## Ôn: ETL best practices

| Practice | Mô tả |
|----------|--------|
| **Idempotent** | Chạy nhiều lần cùng input → cùng output; ON CONFLICT, MERGE, INSERT WHERE NOT EXISTS. |
| **Incremental** | Chỉ xử lý dữ liệu mới (vd: WHERE order_date > last_load_date). |
| **Dedup** | ROW_NUMBER() PARTITION BY business_key ORDER BY updated_at DESC → lấy row_number = 1. |
| **Set-based** | Tránh vòng lặp; dùng INSERT/SELECT, UPDATE FROM. |
| **Transaction** | BEGIN/COMMIT; lỗi thì ROLLBACK. |
| **Naming** | stg_, dim_, fact_; comment bảng/cột. |

---

## Data modeling for dashboard (materialized tables / views)

**Mục đích:** Dashboard query nhanh, ít join phức tạp — dùng bảng/view đã tổng hợp sẵn.

- **Materialized view:** Kết quả query được lưu vật lý; refresh định kỳ (REFRESH MATERIALIZED VIEW).
- **Summary table:** Bảng thường, ETL job ghi đè hoặc incremental (vd: daily_revenue, top_customers).

**Trong lab:** Chạy **`sql/week04_buoi8_materialized_views.sql`** để tạo materialized views (vd: doanh thu theo ngày, top khách, doanh thu theo category). Metabase query trực tiếp các view này cho dashboard nhanh.

---

## Lab 1: ETL pipeline CSV → PostgreSQL → Data mart

**Mục tiêu:** Chạy pipeline đầy đủ: (1) Ingest CSV vào raw, load vào `app`; (2) Refresh data mart `mart` từ `app`; (3) (Tuỳ chọn) Refresh materialized views cho dashboard.

### Cách 1 — Chạy từng bước tay

```bash
export DB_URI="postgresql://de_user:de_pass@localhost:5432/de_db"

# Bước 1: CSV → app (vd: products)
python week03/etl.py week03/sample_data/products.csv

# Bước 2: app → mart (refresh dim + fact)
psql "$DB_URI" -f sql/week02_buoi3_datamart_etl.sql

# Bước 3: Tạo / refresh materialized views (file: DROP + CREATE + REFRESH, idempotent)
psql "$DB_URI" -f sql/week04_buoi8_materialized_views.sql
```

### Cách 2 — Script một lệnh

```bash
./scripts/run_etl_pipeline.sh
```

Script sẽ: tạo raw + copy CSV → chạy Python ETL (CSV → app) → chạy mart ETL (app → mart) → refresh materialized views (nếu có). Xem `scripts/run_etl_pipeline.sh` để chỉnh SOURCE_CSV, DB_URI.

### Kiểm tra

- `app.products` / `app.orders` có data mới (nếu CSV có).
- `mart.fact_sales`, `mart.dim_*` có data tương ứng.
- Materialized views (vd: `app.mv_daily_revenue`) có dữ liệu sau REFRESH.

---

## Lab 2: Dashboard Metabase nâng cao

**Mục tiêu:** Dùng filters, nhiều Question, và (tuỳ chọn) query materialized views để dashboard nhanh và linh hoạt.

### 2.1 Filters trên Dashboard

- Trên Dashboard: **Add filter** (vd: Date, Category, Customer).
- Map filter vào từng Question (chọn column tương ứng).
- Người xem chọn giá trị → Question tự động filter.

### 2.2 Question từ materialized views

- Tạo **Native query** chọn từ `app.mv_daily_revenue` (hoặc view đã tạo trong week04_buoi8_materialized_views.sql).
- So sánh tốc độ với query join trực tiếp từ `app.orders` + `app.customers`.

### 2.3 Nhiều chart trên một Dashboard

- Revenue theo ngày (Line/Bar).
- Top customers (Table/Bar).
- Revenue theo category (Pie/Bar).
- Số đơn theo trạng thái (Scalar / Table).

### 2.4 (Tuỳ chọn) Schedule refresh

- Metabase: **Dashboard subscription** hoặc **Pulse** để gửi báo cáo định kỳ.
- ETL + materialized view: lên lịch cron chạy `run_etl_pipeline.sh` và REFRESH MATERIALIZED VIEW (trong script hoặc cron riêng).

---

## Recap — Tuần 4 & ETL → DW → BI

| Buổi | Nội dung chính |
|------|-----------------|
| **Buổi 7** | Kimball vs Inmon, ETL vs ELT, SCD; Postgres + Metabase; dashboard đầu tiên. |
| **Buổi 8** | ETL best practices; materialized tables cho dashboard; pipeline CSV → app → mart; dashboard nâng cao; recap. |

**Luồng dữ liệu (mini project):**

1. **Source:** CSV (raw).
2. **Ingest:** Shell script copy vào `data/raw/`; Python ETL load vào **PostgreSQL (app)**.
3. **Data mart:** SQL ETL từ **app** → **mart** (dim + fact, SCD2).
4. **Dashboard:** **Metabase** query **app** và **mart** (và materialized views) → Question → Dashboard.
5. **Schedule:** Cron chạy ETL pipeline; (tuỳ chọn) refresh materialized views; Metabase subscription/Pulse.

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Pipeline script | `scripts/run_etl_pipeline.sh` |
| CSV → app | `python week03/etl.py <csv>` |
| app → mart | `psql ... -f sql/week02_buoi3_datamart_etl.sql` |
| Materialized views | `sql/week04_buoi8_materialized_views.sql` |
| Metabase | http://localhost:3000 |
| DB_URI | `postgresql://de_user:de_pass@localhost:5432/de_db` |
