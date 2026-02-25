# Week 3 · Buổi 5 — Python for Data Engineering

**Pandas, typing, error handling · Mini ETL: CSV → clean → PostgreSQL · Unit test với pytest**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Lý thuyết | **Pandas:** đọc CSV, xử lý null, type casting, drop duplicates |
| Lý thuyết | **Typing:** type hints (Optional, List, DataFrame), lợi ích trong ETL |
| Lý thuyết | **Error handling:** try/except, logging, custom exception |
| Lab 1 | **Mini ETL pipeline:** CSV → clean → load PostgreSQL (`app.products`) |
| Lab 2 | **Unit test** bằng pytest (test clean logic, test load với mock/DB) |

---

## Chuẩn bị

- Python 3.10+ (khuyến nghị venv).
- Postgres đang chạy; schema `app` đã có (Buổi 1):

```bash
docker compose up -d postgres
psql postgresql://de_user:de_pass@localhost:5432/de_db -f sql/01_create_oltp_schema.sql
```

- Cài dependency (trong thư mục `week03/`):

```bash
sudo apt install python3.12-venv -y
cd week03
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## Lý thuyết tóm tắt

### Pandas trong ETL

- **Đọc:** `pd.read_csv()` — dtype, na_values, encoding.
- **Clean:** `dropna(subset=[...])`, `astype()`, `fillna()`, `drop_duplicates()`, chuẩn hóa chuỗi (strip, lower).
- **Validate:** kiểm tra schema (cột bắt buộc, kiểu dữ liệu) trước khi load.

### Typing (type hints)

- **Hàm:** `def clean_df(df: pd.DataFrame) -> pd.DataFrame:`
- **Optional / List:** `from typing import Optional, List` — rõ ràng input/output, IDE và mypy hỗ trợ.
- **ETL:** type hint cho pipeline (path: str, conn: connection) giúp đọc code và bắt lỗi sớm.

### Error handling

- **try/except:** bắt lỗi đọc file, kết nối DB, execute SQL; log và raise hoặc return lỗi.
- **Logging:** `logging.info/error` thay vì chỉ print; cấu hình level và format.
- **Custom exception:** vd. `ETLValidationError`, `ETLLoadError` — dễ xử lý theo từng bước pipeline.

---

## Lab 1: Mini ETL — CSV → clean → PostgreSQL

**Mục tiêu:** Đọc CSV sản phẩm (product_name, category, price), clean (null, type, duplicate), load vào `app.products`.

### Cấu trúc code (Week 3)

```
week03/
├── etl.py              # Pipeline: load_csv, clean_df, load_to_pg, run_pipeline
├── sample_data/
│   └── products.csv    # CSV mẫu (có thể có null, sai type, trùng)
├── requirements.txt
└── tests/
    └── test_etl.py     # pytest
```

### Bước 1 — Đọc và chạy pipeline

```bash
cd week03
export DB_URI="postgresql://de_user:de_pass@localhost:5432/de_db"   # hoặc .env
python etl.py sample_data/products.csv
```

Hoặc truyền DB_URI qua tham số: `python etl.py sample_data/products.csv "postgresql://de_user:de_pass@localhost:5432/de_db"`.

### Bước 2 — Logic clean (trong `etl.py`)

- Đọc CSV → DataFrame.
- **Clean:** drop row thiếu `product_name` hoặc `price`; cast `price` sang numeric (coerce errors → drop hoặc fill); strip string; drop_duplicates(subset=[...]).
- **Load:** INSERT vào `app.products` (product_name, category, price); dùng `ON CONFLICT` hoặc ignore duplicate theo business key nếu cần.

### Bước 3 — Kiểm tra trong Postgres

```bash
psql postgresql://de_user:de_pass@localhost:5432/de_db -c "SELECT * FROM app.products ORDER BY product_id DESC LIMIT 10;"
```

---

## Lab 2: Unit test bằng pytest

**Mục tiêu:** Viết test cho hàm clean và (tuỳ chọn) load (mock connection hoặc test DB).

### Cấu trúc test

- **test_clean_df:** Cho DataFrame input (có null, sai type, trùng) → gọi `clean_df()` → assert số dòng, cột, kiểu, không còn null ở cột bắt buộc.
- **test_load_to_pg:** Có thể dùng mock (patch psycopg2/sqlalchemy) hoặc test DB thật; assert số row insert, không raise.
- **test_run_pipeline:** Integration: chạy pipeline với file CSV mẫu, kiểm tra DB có thêm row (hoặc dùng fixture DB tạm).

### Chạy test

```bash
cd week03
pytest tests/ -v
# hoặc
python -m pytest tests/test_etl.py -v
```

---

## Demo flow gợi ý (instructor)

1. **Lý thuyết (20–25 phút):** Pandas (read_csv, clean), typing (hàm ETL), error handling (try/except, logging).
2. **Lab 1 — Mini ETL (30–40 phút):** Mở `etl.py`, giải thích từng bước (load_csv → clean_df → load_to_pg); chạy với `products.csv`; kiểm tra Postgres.
3. **Lab 2 — Pytest (25–30 phút):** Mở `test_etl.py`, giải thích test clean (input/output); chạy pytest; (tuỳ chọn) test load với mock.
4. **Kết (5 phút):** Tóm tắt: clean tách biệt dễ test; typing và logging giúp bảo trì ETL.

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Connection | `postgresql://de_user:de_pass@localhost:5432/de_db` |
| Schema / bảng | `app.products` (product_name, category, price) |
| Thư mục lab | `week03/` |
| File ETL | `week03/etl.py` |
| File test | `week03/tests/test_etl.py` |
| CSV mẫu | `week03/sample_data/products.csv` |
