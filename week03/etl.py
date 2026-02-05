"""
Week 3 · Buổi 5 — Mini ETL: CSV → clean → PostgreSQL (app.products)
Pandas, typing, error handling.
Usage: python etl.py sample_data/products.csv
       DB_URI=postgresql://... python etl.py sample_data/products.csv
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Custom exceptions (error handling)
# -----------------------------------------------------------------------------


class ETLValidationError(Exception):
    """Raised when CSV or DataFrame fails validation."""


class ETLLoadError(Exception):
    """Raised when load to PostgreSQL fails."""


# -----------------------------------------------------------------------------
# ETL steps (typed)
# -----------------------------------------------------------------------------


def load_csv(path: str) -> pd.DataFrame:
    """Read CSV into DataFrame. Raises ETLValidationError if file missing or empty."""
    if not os.path.isfile(path):
        raise ETLValidationError(f"File not found: {path}")
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=True)
    except Exception as e:
        logger.error("Failed to read CSV: %s", e)
        raise ETLValidationError(f"Failed to read CSV: {e}") from e
    if df.empty:
        raise ETLValidationError("CSV is empty")
    required = {"product_name", "category", "price"}
    missing = required - set(df.columns)
    if missing:
        raise ETLValidationError(f"Missing columns: {missing}")
    logger.info("Loaded %d rows from %s", len(df), path)
    return df


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame: drop null product_name/price, coerce price to numeric,
    strip strings, drop duplicates. Returns cleaned DataFrame.
    """
    df = df.copy()
    # Drop rows missing product_name or price
    df = df.dropna(subset=["product_name", "price"])
    # Coerce price to numeric; drop rows that cannot be converted
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price"])
    df["price"] = df["price"].astype("float64")
    # Ensure non-negative price
    df = df[df["price"] >= 0]
    # Strip string columns
    for col in ["product_name", "category"]:
        if col in df.columns and df[col].dtype == object:
            df[col] = df[col].str.strip()
    # Fill category if empty (optional: allow NULL in DB)
    df["category"] = df["category"].fillna("")
    # Drop duplicates on (product_name, category, price)
    before = len(df)
    df = df.drop_duplicates(subset=["product_name", "category", "price"])
    logger.info("Cleaned: %d -> %d rows", before, len(df))
    return df.reset_index(drop=True)


def load_to_pg(df: pd.DataFrame, db_uri: str, schema: str = "app", table: str = "products") -> int:
    """
    Insert DataFrame into PostgreSQL schema.table.
    Expects columns: product_name, category, price.
    Returns number of rows inserted. Raises ETLLoadError on failure.
    """
    if df.empty:
        logger.warning("No rows to load")
        return 0
    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError as e:
        raise ETLLoadError(f"psycopg2 required: {e}") from e

    try:
        conn = psycopg2.connect(db_uri)
    except Exception as e:
        logger.error("DB connection failed: %s", e)
        raise ETLLoadError(f"Connection failed: {e}") from e

    columns = ["product_name", "category", "price"]
    for c in columns:
        if c not in df.columns:
            conn.close()
            raise ETLLoadError(f"DataFrame missing column: {c}")

    q = f'INSERT INTO {schema}.{table} (product_name, category, price) VALUES %s'
    rows = df[columns].to_numpy().tolist()
    try:
        with conn.cursor() as cur:
            execute_values(cur, q, rows)
        conn.commit()
        n = len(rows)
        logger.info("Inserted %d rows into %s.%s", n, schema, table)
        return n
    except Exception as e:
        conn.rollback()
        logger.error("Insert failed: %s", e)
        raise ETLLoadError(f"Insert failed: {e}") from e
    finally:
        conn.close()


def run_pipeline(
    csv_path: str,
    db_uri: Optional[str] = None,
    schema: str = "app",
    table: str = "products",
) -> int:
    """
    Run full pipeline: load_csv -> clean_df -> load_to_pg.
    Returns number of rows loaded. Uses DB_URI env if db_uri is None.
    """
    db_uri = db_uri or os.getenv("DB_URI")
    if not db_uri:
        raise ETLLoadError("DB_URI not set and db_uri not passed")

    df_raw = load_csv(csv_path)
    df_clean = clean_df(df_raw)
    n = load_to_pg(df_clean, db_uri, schema=schema, table=table)
    return n


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python etl.py <path_to_csv> [DB_URI optional if env set]")
        sys.exit(1)
    path = sys.argv[1]
    db_uri = sys.argv[2] if len(sys.argv) > 2 else None
    try:
        n = run_pipeline(path, db_uri=db_uri)
        print(f"Done. Loaded {n} rows.")
    except (ETLValidationError, ETLLoadError) as e:
        logger.error("%s", e)
        sys.exit(1)
