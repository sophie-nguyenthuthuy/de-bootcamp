#!/usr/bin/env bash
# ============================================================================
# Download Flink Kafka SQL connector JAR vào thư mục lib (mount vào Flink container).
# Chạy từ repo root: ./scripts/download_flink_connectors.sh
# Sau đó mount flink/lib vào /opt/flink/lib khi chạy Flink (docker-compose hoặc volume).
# ============================================================================
set -e
FLINK_VERSION="1.18"
KAFKA_CONNECTOR_VERSION="3.2.0-1.18"
LIB_DIR="${1:-flink/lib}"
mkdir -p "$LIB_DIR"

echo "Downloading Flink SQL Kafka connector ${KAFKA_CONNECTOR_VERSION}..."
curl -sL -o "$LIB_DIR/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
  "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"

echo "Done. JAR in $LIB_DIR"
echo "To use with Docker: add volume mount to flink-jobmanager and flink-taskmanager:"
echo "  volumes:"
echo "    - ./flink/lib:/opt/flink/lib"
