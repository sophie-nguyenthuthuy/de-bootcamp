#!/usr/bin/env python3
"""
Week 6 Buổi 12 — Kafka Producer (mẫu).
Chạy: python kafka_producer.py
Cần Kafka đang chạy: docker compose --profile streaming up -d
Bootstrap: localhost:9092
"""
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import sys

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "lab-events"


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    messages = [
        {"event": "click", "page": "/home", "user_id": 1},
        {"event": "view", "page": "/product/1", "user_id": 2},
        {"event": "click", "page": "/cart", "user_id": 1},
    ]
    try:
        for i, msg in enumerate(messages):
            producer.send(TOPIC, value=msg)
            print(f"Sent: {msg}")
        producer.flush()
        print(f"Done. Sent {len(messages)} messages to topic '{TOPIC}'.")
    except KafkaError as e:
        print(f"Kafka error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
