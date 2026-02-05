# Week 8 · Buổi 16 — Data Quality & Data Governance

**Data Quality (Great Expectations, Deequ) · Demo GE · Metadata & Data Governance (DataHub) · Demo DataHub**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Lý thuyết | **Data Quality** — GE, Deequ: expectations, validation |
| Lý thuyết | **Metadata & Data Governance** — catalog, lineage, DataHub |
| Demo 1 | **Great Expectations** — validate bảng `app.orders` (Postgres / CSV) |
| Demo 2 | **DataHub** — chạy quickstart, xem metadata & lineage |

---

## Chuẩn bị

- Python 3.9+ (cho demo GE).
- Postgres đang chạy, có schema `app` và bảng `app.orders` (hoặc dùng CSV mẫu).
- Docker (cho DataHub quickstart).

---

## Lý thuyết tóm tắt

### Data Quality — Great Expectations & Deequ

- **Data Quality:** Đảm bảo dữ liệu đúng chuẩn (completeness, correctness, consistency) qua **expectations** (rule) và **validation** (chạy rule trên batch).
- **Great Expectations (GE):** Thư viện Python; định nghĩa **Expectation Suite** (vd: cột không null, giá trị trong tập hợp, kiểu dữ liệu). Chạy **Checkpoint** trên DataFrame / DB / file → báo **success / failure** và **Data Docs** (HTML).
- **Deequ:** Thư viện Scala (Spark); kiểm tra chất lượng trên dataset lớn (Metrics, Constraints). Dùng trong pipeline Spark (vd: sau khi ghi silver/gold).

| | Great Expectations | Deequ |
|---|-------------------|--------|
| **Nền tảng** | Python (pandas, SQL, Spark) | Scala/Spark |
| **Mô hình** | Expectation Suite + Checkpoint | Metrics + Constraints |
| **Output** | JSON result, Data Docs | Metrics repository, Assertion |

### Metadata & Data Governance — DataHub

- **Metadata:** Thông tin về data (schema, nguồn, owner, lineage, tags).
- **Data Governance:** Chính sách và công cụ quản lý data (access, quality, lifecycle). **Data catalog** = nơi lưu metadata, tìm kiếm, lineage.
- **DataHub:** Open-source metadata platform (LinkedIn). Thu thập metadata từ DB, Kafka, Airflow, Snowflake, …; UI tìm kiếm, lineage, ownership, tags.

---

## Demo 1: Great Expectations — validate `app.orders`

**Mục tiêu:** Chạy script GE: đọc dữ liệu từ Postgres (bảng `app.orders`) hoặc CSV, thêm expectations (not null, value in set), chạy checkpoint và in kết quả.

### Bước 1 — Cài dependency

```bash
cd de-bootcamp/week08
pip install -r requirements.txt
```

### Bước 2 — Chạy script GE

```bash
python ge_demo.py
```

Script sẽ:

1. Đọc `app.orders` từ Postgres (hoặc dùng CSV nếu không kết nối được).
2. Tạo Validator từ DataFrame, thêm expectations (vd: `order_id` not null, `status` in set).
3. Chạy checkpoint và in **success** / **failure** và số expectation pass/fail.

### Bước 3 — (Tuỳ chọn) Xem Data Docs

Script có thể gọi `context.build_data_docs()` để sinh HTML; mở thư mục `great_expectations/uncommitted/data_docs` (hoặc path trong script) để xem report.

### Cấu trúc (week08)

```
week08/
  requirements.txt   # great_expectations, pandas, psycopg2-binary
  ge_demo.py         # Validate app.orders (Postgres hoặc CSV)
```

---

## Demo 2: DataHub — metadata & lineage

**Mục tiêu:** Chạy DataHub quickstart (Docker), truy cập UI, xem metadata và lineage (sau khi ingest từ Postgres / Kafka nếu đã cấu hình).

### Bước 1 — Clone và chạy DataHub quickstart

```bash
git clone https://github.com/datahub-project/datahub.git
cd datahub/docker
./quickstart.sh
```

(Quickstart sẽ chạy GMS, frontend, MySQL, Elasticsearch, Kafka, …; lần đầu có thể mất vài phút.)

### Bước 2 — Truy cập UI

- **URL:** http://localhost:9002  
- **Login:** (mặc định không bắt buộc hoặc admin / admin tùy bản)

### Bước 3 — Xem metadata & lineage

- **Search:** Tìm dataset (vd: bảng Postgres, topic Kafka) nếu đã ingest.
- **Lineage:** Mở một dataset → tab **Lineage** để xem upstream/downstream.
- **Ingest:** Để có metadata từ Postgres/Kafka, chạy **ingestion recipe** (file YAML) — xem [DataHub ingestion](https://datahubproject.io/docs/metadata-ingestion/).

### Bước 4 — (Tuỳ chọn) Ingest Postgres vào DataHub

Từ thư mục `datahub`:

```bash
pip install 'acryl-datahub[postgres]'
datahub ingest -c examples/recipes/postgres_to_datahub.yml
```

(Sửa `postgres_to_datahub.yml` với connection Postgres của bạn.) Sau đó refresh UI để thấy datasets và lineage.

### Lưu ý

- DataHub quickstart dùng nhiều tài nguyên (nhiều container). Có thể dùng **datahub-ingest** standalone để push metadata lên DataHub Cloud hoặc instance tự host nhẹ hơn.
- Trong bootcamp, có thể chỉ **demo UI** (screenshot hoặc instance giáo viên đã chạy sẵn) nếu không chạy full quickstart.

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| GE demo script | `week08/ge_demo.py` |
| GE dependencies | `week08/requirements.txt` (great_expectations, pandas, psycopg2-binary) |
| DataHub quickstart | `git clone ... datahub && cd datahub/docker && ./quickstart.sh` |
| DataHub UI | http://localhost:9002 |
| DataHub ingestion | https://datahubproject.io/docs/metadata-ingestion/ |
