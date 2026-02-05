# Week 3 · Buổi 6 — Unix/Linux & Shell Scripting

**File system · Bash commands · Cron jobs · Shell script tự động ingest raw data · Cron schedule ETL**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Lý thuyết | **File system:** cấu trúc thư mục, path, quyền (chmod, chown) |
| Lý thuyết | **Bash commands:** cd, ls, cp, mv, mkdir, grep, find, xargs, pipe, redirect |
| Lý thuyết | **Cron jobs:** crontab, cú pháp (phút giờ ngày tháng thứ), log và exit code |
| Lab 1 | **Viết shell script tự động ingest raw data** (copy/lưu raw → gọi ETL load Postgres) |
| Lab 2 | **Cronjob schedule ETL** (chạy script theo lịch, kiểm tra log) |

---

## Chuẩn bị

- Môi trường Unix/Linux hoặc macOS (hoặc WSL2 trên Windows).
- Postgres đang chạy; schema `app` đã có (Buổi 1).
- Python ETL Buổi 5 đã có: `week03/etl.py`, `week03/sample_data/products.csv`.
- Cài dependency: `pip install -r week03/requirements.txt`.

---

## Lý thuyết tóm tắt

### File system

- **Path:** absolute (`/home/user/project`), relative (`./scripts`, `../data`).
- **Thư mục thường dùng:** `data/raw/` (raw input), `data/processed/`, `logs/`.
- **Quyền:** `chmod +x script.sh` (cho phép thực thi); `ls -la` xem quyền.

### Bash commands thường dùng trong ETL

| Lệnh | Mô tả |
|------|--------|
| `cd`, `pwd` | Di chuyển, in thư mục hiện tại |
| `mkdir -p dir` | Tạo thư mục (kể cả cha) |
| `cp src dst` | Copy file |
| `mv src dst` | Di chuyển / đổi tên |
| `date +%Y%m%d` | Ngày để đặt tên file (vd: raw_20250204.csv) |
| `grep`, `find` | Tìm file / nội dung |
| `>> log.txt` | Ghi thêm vào file (append) |
| `2>&1` | Gộp stderr vào stdout |
| `set -euo pipefail` | Script dừng khi lỗi, biến chưa set, pipe lỗi |

### Cron jobs

- **Crontab:** `crontab -e` — chỉnh sửa lịch chạy.
- **Cú pháp:** `phút giờ ngày_tháng tháng thứ_tuần lệnh`
  - Ví dụ: `0 2 * * *` = 02:00 mỗi ngày; `*/15 * * * *` = mỗi 15 phút.
- **Lưu ý:** dùng đường dẫn tuyệt đối; set `PATH` hoặc gọi full path; log ra file (vd: `>> /path/log 2>&1`); exit code 0 = thành công (cron gửi mail nếu script in ra stderr / exit khác 0, tùy cấu hình).

---

## Lab 1: Shell script tự động ingest raw data

**Mục tiêu:** Viết script Bash: (1) tạo thư mục raw nếu chưa có, (2) ingest raw data (copy CSV vào raw với tên có timestamp), (3) gọi Python ETL load vào Postgres, (4) ghi log và trả exit code phù hợp.

### Cấu trúc script (`scripts/ingest_raw_and_etl.sh`)

- **RAW_DIR:** thư mục lưu file raw (vd: `data/raw`).
- **LOG_FILE:** log từng lần chạy (vd: `logs/ingest_etl.log`).
- **Bước 1:** `mkdir -p "$RAW_DIR"`, `mkdir -p logs`.
- **Bước 2:** Ingest raw — copy nguồn (vd: `week03/sample_data/products.csv`) sang `$RAW_DIR/products_$(date +%Y%m%d_%H%M%S).csv`.
- **Bước 3:** Gọi `python week03/etl.py "$raw_file"` (cần `DB_URI`); bắt lỗi, ghi log, exit 1 nếu thất bại.
- **Bước 4:** Ghi log thành công, exit 0.

### Chạy thử (từ repo root)

```bash
cd /path/to/de-bootcamp
chmod +x scripts/ingest_raw_and_etl.sh
export DB_URI="postgresql://de_user:de_pass@localhost:5432/de_db"
./scripts/ingest_raw_and_etl.sh
```

Kiểm tra: `data/raw/` có file mới; `logs/ingest_etl.log` có log; Postgres `app.products` có thêm dòng (nếu CSV có dữ liệu mới).

---

## Lab 2: Cronjob schedule ETL

**Mục tiêu:** Lên lịch chạy script ingest + ETL bằng cron (vd: mỗi ngày 02:00).

### Bước 1 — Lấy đường dẫn tuyệt đối và biến môi trường

Script cần chạy với **working directory** và **PATH** ổn định. Trong script nên:

- Chuyển về thư mục gốc repo: `cd "$(dirname "$0")/.."` (nếu script nằm trong `scripts/`) hoặc set `PROJECT_ROOT`.
- Export `DB_URI` trong script (hoặc đọc từ file `.env` / cấu hình) để cron không phụ thuộc shell profile.

### Bước 2 — Thêm cron job

```bash
crontab -e
```

Thêm dòng (sửa `/path/to/de-bootcamp` thành đường dẫn thật):

```cron
# Chạy ingest + ETL mỗi ngày lúc 02:00
0 2 * * * cd /path/to/de-bootcamp && DB_URI="postgresql://de_user:de_pass@localhost:5432/de_db" ./scripts/ingest_raw_and_etl.sh >> logs/ingest_etl.log 2>&1
```

Hoặc chạy mỗi 15 phút (để test nhanh):

```cron
*/15 * * * * cd /path/to/de-bootcamp && DB_URI="..." ./scripts/ingest_raw_and_etl.sh >> logs/ingest_etl.log 2>&1
```

### Bước 3 — Kiểm tra

- Đợi đến thời điểm cron chạy (hoặc dùng `*/1 * * * *` để chạy mỗi phút khi test).
- Xem log: `tail -f logs/ingest_etl.log`.
- Kiểm tra `data/raw/` có file mới; `app.products` có tăng số dòng (nếu CSV có dữ liệu mới).

### File mẫu crontab (để tham khảo)

Script **`scripts/crontab.example`** (trong repo) chứa ví dụ crontab; không tự động cài — học viên copy nội dung vào `crontab -e` và sửa path/DB_URI.

---

## Demo flow gợi ý (instructor)

1. **Lý thuyết (20–25 phút):** File system (raw/processed/logs), bash (set -e, pipe, redirect), cron (cú pháp, log, exit code).
2. **Lab 1 — Shell script (30–35 phút):** Mở `ingest_raw_and_etl.sh`, giải thích từng bước; chạy tay; kiểm tra raw + log + Postgres.
3. **Lab 2 — Cron (15–20 phút):** Mở crontab.example; thêm job (vd. mỗi phút để test); xem log; nhắc nhở sửa path và DB_URI.
4. **Kết (5 phút):** Tóm tắt: script idempotent/cron-friendly, log và exit code quan trọng cho scheduling.

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Script ingest + ETL | `scripts/ingest_raw_and_etl.sh` |
| Raw data thư mục | `data/raw/` |
| Log file | `logs/ingest_etl.log` |
| Crontab mẫu | `scripts/crontab.example` |
| DB_URI | `postgresql://de_user:de_pass@localhost:5432/de_db` |
| Python ETL | `week03/etl.py` |
