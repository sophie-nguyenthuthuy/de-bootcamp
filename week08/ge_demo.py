#!/usr/bin/env python3
"""
Week 8 Buổi 16 — Demo Great Expectations: validate app.orders (Postgres hoặc CSV).
Chạy: python ge_demo.py
Cần: Postgres đang chạy với app.orders (hoặc đặt USE_CSV=1 và có file sample_orders.csv).
"""
import os
import sys

import pandas as pd

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "de_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "de_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "de_pass")
USE_CSV = os.environ.get("USE_CSV", "0").strip().lower() in ("1", "true", "yes")
CSV_PATH = os.environ.get("CSV_PATH", os.path.join(os.path.dirname(__file__), "sample_orders.csv"))


def load_orders_from_postgres() -> pd.DataFrame:
    import psycopg2
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    query = """
        SELECT order_id, customer_id, order_date, total_amount, status, created_at
        FROM app.orders
        LIMIT 1000
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def load_orders_from_csv() -> pd.DataFrame:
    if not os.path.exists(CSV_PATH):
        # Tạo CSV mẫu nhỏ nếu không có
        df = pd.DataFrame({
            "order_id": [1, 2, 3],
            "customer_id": [1, 1, 2],
            "order_date": ["2025-01-01", "2025-01-02", "2025-01-03"],
            "total_amount": [100.0, 200.0, 150.0],
            "status": ["created", "confirmed", "created"],
            "created_at": ["2025-01-01 10:00:00", "2025-01-02 11:00:00", "2025-01-03 12:00:00"],
        })
        df.to_csv(CSV_PATH, index=False)
        print(f"Created sample CSV: {CSV_PATH}", file=sys.stderr)
    return pd.read_csv(CSV_PATH)


def main() -> None:
    import great_expectations as gx

    # 1. Load data
    if USE_CSV:
        df = load_orders_from_csv()
    else:
        try:
            df = load_orders_from_postgres()
        except Exception as e:
            print(f"Postgres failed ({e}), falling back to CSV.", file=sys.stderr)
            USE_CSV = True
            df = load_orders_from_csv()

    if df.empty:
        print("No data to validate.")
        sys.exit(0)

    print(f"Loaded {len(df)} rows.")

    # 2. Great Expectations context & validator
    context = gx.get_context()
    validator = context.sources.add_pandas("orders_datasource").read_dataframe(
        df,
        asset_name="orders_asset",
        batch_metadata={"source": "postgres" if not USE_CSV else "csv"},
    )

    # 3. Add expectations (Data Quality rules)
    validator.expect_column_values_to_not_be_null("order_id")
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_be_in_set(
        "status",
        value_set=["created", "confirmed", "shipped", "cancelled"],
        mostly=1.0,
    )
    validator.expect_column_values_to_be_between(
        "total_amount",
        min_value=0,
        max_value=1e9,
        mostly=1.0,
    )
    validator.expect_table_row_count_to_be_between(min_value=1, max_value=1_000_000)

    # 4. Save suite & run checkpoint
    validator.save_expectation_suite()
    checkpoint = context.add_or_update_checkpoint(
        name="orders_checkpoint",
        validator=validator,
    )
    result = checkpoint.run()

    # 5. Print result
    if result["success"]:
        print("Validation SUCCESS: all expectations passed.")
    else:
        print("Validation FAILED: some expectations did not pass.")
        for r in result.get("run_results", {}).values():
            vr = r.get("validation_result", {})
            for exp in vr.get("results", []):
                if not exp.get("success", True):
                    print(f"  - {exp.get('expectation_config', {}).get('expectation_type')}: {exp.get('result', {})}")
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
