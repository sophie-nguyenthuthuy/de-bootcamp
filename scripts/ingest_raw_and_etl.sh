#!/usr/bin/env bash
# =============================================================================
# Week 3 · Buổi 6 — Ingest raw data + run ETL (cron-friendly)
# - Create data/raw, logs
# - Copy source CSV to data/raw with timestamp
# - Run week03/etl.py to load into app.products
# - Log and exit 0 (success) or 1 (failure) for cron
#
# Usage (from repo root):
#   export DB_URI="postgresql://de_user:de_pass@localhost:5432/de_db"
#   ./scripts/ingest_raw_and_etl.sh
#
# Cron example:
#   0 2 * * * cd /path/to/de-bootcamp && DB_URI="..." ./scripts/ingest_raw_and_etl.sh >> logs/ingest_etl.log 2>&1
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RAW_DIR="${RAW_DIR:-$REPO_ROOT/data/raw}"
LOG_DIR="${LOG_DIR:-$REPO_ROOT/logs}"
LOG_FILE="${LOG_FILE:-$LOG_DIR/ingest_etl.log}"
# Source CSV to ingest (default: week03 sample)
SOURCE_CSV="${SOURCE_CSV:-$REPO_ROOT/week03/sample_data/products.csv}"
DB_URI="${DB_URI:-postgresql://de_user:de_pass@localhost:5432/de_db}"

mkdir -p "$RAW_DIR"
mkdir -p "$LOG_DIR"

timestamp() { date +"%Y-%m-%d %H:%M:%S"; }
log()      { echo "[$(timestamp)] $*" | tee -a "$LOG_FILE"; }
log_info() { log "[INFO] $*"; }
log_error() { log "[ERROR] $*"; }

log_info "Starting ingest_raw_and_etl (REPO_ROOT=$REPO_ROOT)"

# -----------------------------------------------------------------------------
# 1. Ingest raw: copy source CSV to raw dir with timestamp
# -----------------------------------------------------------------------------
if [[ ! -f "$SOURCE_CSV" ]]; then
  log_error "Source CSV not found: $SOURCE_CSV"
  exit 1
fi

RAW_FILE="$RAW_DIR/products_$(date +%Y%m%d_%H%M%S).csv"
cp "$SOURCE_CSV" "$RAW_FILE"
log_info "Ingested raw file: $RAW_FILE"

# -----------------------------------------------------------------------------
# 2. Run Python ETL (week03/etl.py)
# -----------------------------------------------------------------------------
cd "$REPO_ROOT"
if [[ ! -f week03/etl.py ]]; then
  log_error "week03/etl.py not found"
  exit 1
fi

if ! command -v python3 &>/dev/null && ! command -v python &>/dev/null; then
  log_error "python3/python not found"
  exit 1
fi

PYTHON="${PYTHON:-python3}"
if ! command -v "$PYTHON" &>/dev/null; then
  PYTHON=python
fi

log_info "Running ETL: $PYTHON week03/etl.py $RAW_FILE"
if ! DB_URI="$DB_URI" "$PYTHON" week03/etl.py "$RAW_FILE" >> "$LOG_FILE" 2>&1; then
  log_error "ETL failed (exit code $?)"
  exit 1
fi

log_info "Ingest + ETL completed successfully"
exit 0
