#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# ADVANCED INGESTION PIPELINE SCRIPT
#   - Init OLTP schema
#   - Seed sample data
#   - Trigger Airflow DAG (Postgres → MinIO)
#   - Auto-verify output in MinIO via mc CLI
#   - Trigger Spark job (Bronze → Silver → Gold)
#   - Optional scheduling loop (--loop)
#
# Log file: logs/ingestion.log
# Usage:
#   ./scripts/automate_ingestion.sh
#   ./scripts/automate_ingestion.sh --loop  (simulate cron every 5 min)
# ============================================================

DB_URI=${DB_URI:-"postgresql://de_user:de_pass@localhost:5432/de_db"}
AIRFLOW_CONTAINER=${AIRFLOW_CONTAINER:-"de_airflow"}
SPARK_CONTAINER=${SPARK_CONTAINER:-"de_spark_master"}
LOG_DIR="logs"
LOG_FILE="$LOG_DIR/ingestion.log"
MC_ALIAS="local"

BOLD=$(tput bold)
RESET=$(tput sgr0)
GREEN=$(tput setaf 2)
YELLOW=$(tput setaf 3)
RED=$(tput setaf 1)

mkdir -p "$LOG_DIR"

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

log() {
  echo -e "[$(timestamp)] $1" | tee -a "$LOG_FILE"
}

log_info() { log "${GREEN}[INFO]${RESET} $1"; }
log_warn() { log "${YELLOW}[WARN]${RESET} $1"; }
log_error(){ log "${RED}[ERROR]${RESET} $1"; }

# ============================================================
# HELPER: CHECK CONTAINER IS RUNNING
# ============================================================
check_container() {
  local name=$1
  if ! docker ps --format '{{.Names}}' | grep -q "$name"; then
    log_error "Container '$name' is not running."
    return 1
  fi
  return 0
}

# ============================================================
# STEP 1: RUN DB INIT
# ============================================================
run_db_init() {
  log_info "Initializing DB schema..."
  psql "$DB_URI" -f sql/01_create_oltp_schema.sql

  log_info "Seeding sample data..."
  psql "$DB_URI" -f sql/02_seed_sample_data.sql
}

# ============================================================
# STEP 2: TRIGGER AIRFLOW DAG
# ============================================================
run_airflow_etl() {
  log_info "Triggering Airflow DAG: postgres_to_minio_etl"

  if ! check_container "$AIRFLOW_CONTAINER"; then
    log_warn "Skipping Airflow ETL because Airflow is not running."
    return
  fi

  docker exec "$AIRFLOW_CONTAINER" airflow dags trigger postgres_to_minio_etl || \
    log_warn "Unable to trigger DAG – it may not be loaded yet."

  log_info "Waiting 12 seconds for Airflow ETL to finish..."
  sleep 12
}

# ============================================================
# STEP 3: VERIFY MINIO OUTPUT USING MC CLI
# ============================================================
verify_minio() {
  log_info "Verifying MinIO output using mc CLI..."

  # Setup mc alias
  docker run --rm \
    --network=de-bootcamp_de-net \
    -v "$PWD":/work \
    minio/mc \
    mc alias set $MC_ALIAS http://de_minio:9000 minioadmin minioadmin \
    >/dev/null 2>&1 || true

  # List objects
  docker run --rm \
    --network=de-bootcamp_de-net \
    minio/mc \
    mc ls $MC_ALIAS/lakehouse/bronze/orders/ \
    || log_warn "MinIO verification: object not found."

  log_info "MinIO verification complete."
}

# ============================================================
# STEP 4: RUN SPARK BRONZE → SILVER → GOLD
# ============================================================
run_spark_job() {
  log_info "Running Spark Bronze → Silver → Gold job..."

  if ! check_container "$SPARK_CONTAINER"; then
    log_warn "Skipping Spark job because Spark is not running."
    return
  fi

  docker exec "$SPARK_CONTAINER" \
    spark-submit \
      --master spark://spark-master:7077 \
      /opt/airflow/spark_jobs/bronze_to_silver_gold.py \
      s3a://lakehouse/bronze/orders/orders_extract.csv \
      s3a://lakehouse/silver/orders_clean/ \
      s3a://lakehouse/gold/orders_daily_metrics/ \
      http://minio:9000 \
      minioadmin \
      minioadmin \
      | tee -a "$LOG_FILE"

  log_info "Spark job completed."
}

# ============================================================
# FULL INGESTION PIPELINE (one run)
# ============================================================
run_pipeline() {
  log_info "================ PIPELINE START ================"

  run_db_init
  run_airflow_etl
  verify_minio
  run_spark_job

  log_info "================ PIPELINE END =================="
}

# ============================================================
# OPTIONAL CRON SIMULATION (LOOP MODE)
# ============================================================
run_loop() {
  log_info "Entering loop mode (run every 5 minutes)..."
  while true; do
    run_pipeline
    log_info "Sleeping 5 minutes..."
    sleep 300  # 5 minutes
  done
}

# ============================================================
# MAIN ENTRYPOINT
# ============================================================
if [[ "${1:-}" == "--loop" ]]; then
  run_loop
else
  run_pipeline
fi
