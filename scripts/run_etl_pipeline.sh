#!/usr/bin/env bash
# =============================================================================
# Week 4 · Buổi 8 — Full ETL pipeline: CSV → PostgreSQL (app) → Data mart (mart)
# - Ingest raw CSV (copy to data/raw) + load to app (week03 ETL)
# - Refresh mart from app (week02_buoi3_datamart_etl.sql)
# - Refresh materialized views for dashboard (week04_buoi8_materialized_views.sql)
#
# Usage (from repo root):
#   export DB_URI="postgresql://de_user:de_pass@localhost:5432/de_db"
#   ./scripts/run_etl_pipeline.sh
#   ./scripts/run_etl_pipeline.sh path/to/extra.csv   # optional: extra CSV to load
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DB_URI="${DB_URI:-postgresql://de_user:de_pass@localhost:5432/de_db}"
SOURCE_CSV="${1:-$REPO_ROOT/week03/sample_data/products.csv}"
LOG_DIR="${LOG_DIR:-$REPO_ROOT/logs}"
LOG_FILE="${LOG_FILE:-$LOG_DIR/etl_pipeline.log}"

mkdir -p "$LOG_DIR"
timestamp() { date +"%Y-%m-%d %H:%M:%S"; }
log() { echo "[$(timestamp)] $*" | tee -a "$LOG_FILE"; }

log "Starting ETL pipeline (REPO_ROOT=$REPO_ROOT)"

# -----------------------------------------------------------------------------
# 1. CSV → app (Python ETL)
# -----------------------------------------------------------------------------
cd "$REPO_ROOT"
if [[ -f "$SOURCE_CSV" ]] && [[ -f week03/etl.py ]]; then
  log "Step 1: Load CSV to app.products — $SOURCE_CSV"
  if DB_URI="$DB_URI" python3 week03/etl.py "$SOURCE_CSV" >> "$LOG_FILE" 2>&1; then
    log "Step 1 OK: CSV loaded to app"
  else
    log "Step 1 WARN: Python ETL failed (non-fatal if products already exist)"
  fi
else
  log "Step 1 SKIP: CSV or week03/etl.py not found"
fi

# -----------------------------------------------------------------------------
# 2. app → mart (SQL ETL)
# -----------------------------------------------------------------------------
if [[ -f sql/week02_buoi3_datamart_etl.sql ]]; then
  log "Step 2: Refresh mart from app"
  if psql "$DB_URI" -f sql/week02_buoi3_datamart_etl.sql >> "$LOG_FILE" 2>&1; then
    log "Step 2 OK: Mart refreshed"
  else
    log "Step 2 ERROR: Mart ETL failed"
    exit 1
  fi
else
  log "Step 2 SKIP: sql/week02_buoi3_datamart_etl.sql not found"
fi

# -----------------------------------------------------------------------------
# 3. Refresh materialized views (dashboard)
# -----------------------------------------------------------------------------
if [[ -f sql/week04_buoi8_materialized_views.sql ]]; then
  log "Step 3: Refresh materialized views"
  if psql "$DB_URI" -f sql/week04_buoi8_materialized_views.sql >> "$LOG_FILE" 2>&1; then
    log "Step 3 OK: Materialized views refreshed"
  else
    log "Step 3 WARN: Materialized views refresh failed (check app schema exists)"
  fi
else
  log "Step 3 SKIP: sql/week04_buoi8_materialized_views.sql not found"
fi

log "ETL pipeline finished"
exit 0
