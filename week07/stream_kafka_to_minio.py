#!/usr/bin/env python3
"""
Week 7 Buổi 14 — Streaming pipeline: Kafka → (transform) → MinIO.
Đọc topic lab-events, thêm processed_at, ghi JSON lên MinIO lakehouse/bronze/stream/.

Chạy: python stream_kafka_to_minio.py
Cần: Kafka + MinIO đang chạy; topic lab-events có dữ liệu (hoặc producer gửi song song).
"""
from datetime import datetime, timezone
import json
import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from minio import Minio

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "lab-events"
GROUP_ID = "stream-to-minio"
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS = "minioadmin"
MINIO_SECRET = "minioadmin"
BUCKET = "lakehouse"
PREFIX = "bronze/stream"


def main() -> None:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS,
        secret_key=MINIO_SECRET,
        secure=False,
    )
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: m.decode("utf-8") if m else None,
    )
    try:
        print(f"Stream pipeline: {TOPIC} → s3://{BUCKET}/{PREFIX}/ (Ctrl+C to stop)")
        for message in consumer:
            raw = message.value
            if raw is None:
                continue
            try:
                payload = json.loads(raw) if raw.strip().startswith("{") else {"raw": raw}
            except json.JSONDecodeError:
                payload = {"raw": raw}
            payload["processed_at"] = datetime.now(timezone.utc).isoformat()
            payload["_partition"] = message.partition
            payload["_offset"] = message.offset
            value_str = json.dumps(payload)
            value_bytes = value_str.encode("utf-8")
            now = datetime.utcnow()
            object_name = f"{PREFIX}/{now.strftime('%Y-%m-%d')}/{now.strftime('%H-%M-%S')}-p{message.partition}-o{message.offset}.json"
            try:
                client.put_object(
                    BUCKET,
                    object_name,
                    data=value_bytes,
                    length=len(value_bytes),
                    content_type="application/json",
                )
                print(f"Written: {object_name}")
            except Exception as e:
                print(f"MinIO error: {e}", file=sys.stderr)
    except KafkaError as e:
        print(f"Kafka error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
