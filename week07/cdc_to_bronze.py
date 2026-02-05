#!/usr/bin/env python3
"""
Week 7 Buổi 13 — Stream CDC events from Kafka to MinIO bronze layer.
Consume topic dbserver1.app.orders (Debezium) and write each message value
as a JSON file to MinIO: lakehouse/bronze/cdc/orders/<date>/<time>-<partition>-<offset>.json

Chạy: python cdc_to_bronze.py
Cần: Kafka + Connect + Debezium connector đang chạy; MinIO đang chạy.
"""
from datetime import datetime
import json
import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from minio import Minio

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "dbserver1.app.orders"
GROUP_ID = "cdc-bronze"
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS = "minioadmin"
MINIO_SECRET = "minioadmin"
BUCKET = "lakehouse"
PREFIX = "bronze/cdc/orders"


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
        value_deserializer=lambda m: m.decode("utf-8"),
    )
    try:
        print(f"CDC → Bronze. Topic: {TOPIC}, group: {GROUP_ID}. Writing to s3://{BUCKET}/{PREFIX}/ (Ctrl+C to stop)")
        for message in consumer:
            value_str = message.value
            if not value_str:
                continue
            now = datetime.utcnow()
            date_dir = now.strftime("%Y-%m-%d")
            object_name = f"{PREFIX}/{date_dir}/{now.strftime('%H-%M-%S')}-p{message.partition}-o{message.offset}.json"
            try:
                value_bytes = value_str.encode("utf-8")
                client.put_object(
                    BUCKET,
                    object_name,
                    data=value_bytes,
                    length=len(value_bytes),
                    content_type="application/json",
                )
                print(f"Written: {object_name}")
            except Exception as e:
                print(f"MinIO error for {object_name}: {e}", file=sys.stderr)
    except KafkaError as e:
        print(f"Kafka error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
