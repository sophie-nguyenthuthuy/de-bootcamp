#!/usr/bin/env python3
"""
Week 6 Buổi 12 — Kafka Consumer (mẫu).
Chạy: python kafka_consumer.py
Cần Kafka đang chạy: docker compose --profile streaming up -d
Bootstrap: localhost:9092
Đọc từ topic lab-events, group lab-group; Ctrl+C để thoát.
"""
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import sys

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "lab-events"
GROUP_ID = "lab-group"


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    try:
        print(f"Consumer started. Topic: {TOPIC}, group: {GROUP_ID}. Waiting for messages... (Ctrl+C to stop)")
        for message in consumer:
            print(f"Partition {message.partition} | Offset {message.offset} | {message.value}")
    except KafkaError as e:
        print(f"Kafka error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
