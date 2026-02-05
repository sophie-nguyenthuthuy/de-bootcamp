# Week 6 · Buổi 12 — Kafka Fundamentals

**Broker · Topic · Consumer Group · Offset · Partitions · Lab: Deploy Kafka + Zookeeper · Lab: Producer & Consumer CLI + Python**

---

## Nội dung buổi học

| Phần | Nội dung |
|------|----------|
| Lý thuyết | **Broker, Topic, Consumer Group** — kiến trúc, vai trò |
| Lý thuyết | **Offset, Partitions** — thứ tự, parallelism |
| Lab 1 | **Deploy Kafka + Zookeeper với Docker** — profile streaming, tạo topic |
| Lab 2 | **Producer & Consumer CLI** — kafka-console-producer/consumer |
| Lab 3 | **Producer & Consumer Python** — kafka-python gửi/nhận message |

---

## Chuẩn bị

- Docker đang chạy.
- Repo đã clone; `docker-compose.yml` có sẵn service `zookeeper` và `kafka` (profile `streaming`).
- Python 3.9+ (cho Lab 3).

---

## Lý thuyết tóm tắt

### Broker, Topic, Consumer Group

- **Broker:** Một node Kafka (server) lưu topic, nhận produce và serve consume. Cluster = nhiều broker.
- **Topic:** Luồng dữ liệu có tên; producer gửi message vào topic, consumer đọc từ topic.
- **Consumer Group:** Nhóm consumer cùng đọc một topic; mỗi partition chỉ gán cho một consumer trong group (load balance). Group có `group.id`; offset được lưu theo group.

### Offset, Partitions

- **Partition:** Topic được chia thành nhiều partition (shard); message có key thì hash key → partition; không key thì round-robin. Tăng partition → tăng parallelism (nhiều consumer trong group).
- **Offset:** Vị trí message trong partition (0, 1, 2, …). Consumer commit offset theo group để biết đã đọc đến đâu; có thể đọc từ đầu (`earliest`) hoặc mới nhất (`latest`).

### Thuật ngữ nhanh

| Thuật ngữ | Ý nghĩa |
|-----------|---------|
| Broker | Node Kafka, lưu và serve dữ liệu topic |
| Topic | Luồng message có tên |
| Partition | Phân đoạn của topic (shard) |
| Offset | Vị trí message trong partition |
| Producer | Client gửi message vào topic |
| Consumer | Client đọc message từ topic |
| Consumer Group | Nhóm consumer chia partition, commit offset chung |
| Replication | Số bản copy partition (fault tolerance) |

---

## Lab 1: Deploy Kafka + Zookeeper với Docker

**Mục tiêu:** Chạy Zookeeper và Kafka bằng Docker Compose (profile `streaming`), tạo topic và kiểm tra.

### Bước 1 — Khởi động Zookeeper + Kafka

```bash
cd de-bootcamp
docker compose --profile streaming up -d
```

Đợi vài giây cho Zookeeper và Kafka sẵn sàng.

### Bước 2 — Kiểm tra container

```bash
docker compose --profile streaming ps
# zookeeper (de_zookeeper), kafka (de_kafka) phải Up
```

### Bước 3 — Tạo topic (chạy trong container Kafka)

```bash
docker exec -it de_kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 2 \
  --topic lab-events
```

- `--partitions 2`: topic có 2 partition (song song).
- `--replication-factor 1`: 1 bản (đủ cho lab single broker).

### Bước 4 — Liệt kê topic

```bash
docker exec -it de_kafka kafka-topics --list --bootstrap-server localhost:9092
```

Cần thấy `lab-events`.

### Bước 5 — Mô tả topic (partition, leader)

```bash
docker exec -it de_kafka kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic lab-events
```

---

## Lab 2: Producer & Consumer CLI

**Mục tiêu:** Dùng công cụ CLI có sẵn trong container để gửi và nhận message.

### Cách 1 — Chạy CLI trong container (bootstrap: localhost:9092)

**Terminal 1 — Consumer (đọc từ đầu):**

```bash
docker exec -it de_kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic lab-events \
  --from-beginning
```

**Terminal 2 — Producer:**

```bash
docker exec -it de_kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic lab-events
```

Gõ vài dòng (mỗi dòng = 1 message), Enter. Consumer sẽ in ra.

### Cách 2 — Chạy CLI trên máy host (cần cài Kafka binaries hoặc dùng Docker)

Từ máy host, bootstrap phải là **localhost:9092** (port đã map).

Ví dụ producer bằng Docker (không cần cài Kafka):

```bash
docker exec -it de_kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic lab-events
```

Consumer tương tự; cả hai đều chạy trong container, kết nối tới broker qua localhost:9092.

---

## Lab 3: Producer & Consumer Python

**Mục tiêu:** Gửi và nhận message bằng thư viện `kafka-python` (chạy trên máy host, kết nối `localhost:9092`).

### Bước 1 — Cài dependency

```bash
cd de-bootcamp/week06
pip install -r requirements.txt
```

### Bước 2 — Chạy consumer (nền)

```bash
python kafka_consumer.py
```

Giữ terminal mở; script đọc từ topic `lab-events`, group `lab-group`, in từng message.

### Bước 3 — Chạy producer (terminal khác)

```bash
python kafka_producer.py
```

Script gửi vài message mẫu vào `lab-events`. Consumer sẽ in ra.

### Bước 4 — (Tuỳ chọn) Gửi message tùy ý

Sửa `kafka_producer.py` để gửi nội dung từ input hoặc file; hoặc dùng vòng lặp + `input()`.

### Cấu trúc thư mục

```
week06/
  requirements.txt   # kafka-python
  kafka_producer.py  # Producer mẫu
  kafka_consumer.py  # Consumer mẫu (group: lab-group)
```

---

## Tham chiếu nhanh

| Mục | Giá trị |
|-----|---------|
| Khởi động Kafka stack | `docker compose --profile streaming up -d` |
| Bootstrap (host) | `localhost:9092` |
| Bootstrap (trong container) | `localhost:9092` hoặc `kafka:9092` (giữa container) |
| Topic mẫu | `lab-events` (2 partition) |
| Tạo topic | `kafka-topics --create --bootstrap-server localhost:9092 --topic Tên --partitions N --replication-factor 1` |
| Producer CLI | `kafka-console-producer --bootstrap-server localhost:9092 --topic Tên` |
| Consumer CLI | `kafka-console-consumer --bootstrap-server localhost:9092 --topic Tên --from-beginning` |
| Python bootstrap | `localhost:9092` (trong script) |

---

## Demo flow gợi ý (instructor)

1. **Lý thuyết (20–25 phút):** Broker, Topic, Partition, Offset, Consumer Group; vẽ sơ đồ producer → topic (partitions) → consumer group.
2. **Lab 1 (15 phút):** `docker compose --profile streaming up -d`, tạo topic `lab-events`, describe topic.
3. **Lab 2 (15 phút):** Mở 2 terminal: consumer `--from-beginning`, producer gõ message; quan sát message xuất hiện ở consumer.
4. **Lab 3 (20 phút):** Chạy Python consumer, chạy Python producer; giải thích group id, offset, partition.
5. **Kết (5 phút):** Tóm tắt: Kafka = log phân tán, partition = parallelism, consumer group = scale + offset.
