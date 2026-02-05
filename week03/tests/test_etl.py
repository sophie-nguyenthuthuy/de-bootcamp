"""
Week 3 · Buổi 5 — Unit tests for mini ETL (pytest).
Run: pytest week03/tests/ -v   (from repo root)
      pytest tests/ -v         (from week03/)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

# Allow importing etl from parent
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl import ETLValidationError, ETLLoadError, clean_df, load_csv, load_to_pg, run_pipeline


# -----------------------------------------------------------------------------
# load_csv
# -----------------------------------------------------------------------------


def test_load_csv_file_not_found():
    with pytest.raises(ETLValidationError, match="File not found"):
        load_csv("nonexistent.csv")


def test_load_csv_success(tmp_path):
    csv = tmp_path / "products.csv"
    csv.write_text("product_name,category,price\nA,Electronics,1000\nB,Laptop,2000")
    df = load_csv(str(csv))
    assert len(df) == 2
    assert list(df.columns) == ["product_name", "category", "price"]
    assert df["product_name"].tolist() == ["A", "B"]


def test_load_csv_missing_columns(tmp_path):
    csv = tmp_path / "bad.csv"
    csv.write_text("name,value\nx,1")
    with pytest.raises(ETLValidationError, match="Missing columns"):
        load_csv(str(csv))


# -----------------------------------------------------------------------------
# clean_df
# -----------------------------------------------------------------------------


def test_clean_df_drops_null_product_name():
    df = pd.DataFrame({
        "product_name": ["A", None, "C"],
        "category": ["X", "X", "X"],
        "price": ["100", "200", "300"],
    })
    out = clean_df(df)
    assert len(out) == 2
    assert out["product_name"].tolist() == ["A", "C"]


def test_clean_df_drops_null_price():
    df = pd.DataFrame({
        "product_name": ["A", "B", "C"],
        "category": ["X", "X", "X"],
        "price": ["100", None, "300"],
    })
    out = clean_df(df)
    assert len(out) == 2
    assert out["price"].tolist() == [100.0, 300.0]


def test_clean_df_coerces_invalid_price_to_nan_then_drops():
    df = pd.DataFrame({
        "product_name": ["A", "B", "C"],
        "category": ["X", "X", "X"],
        "price": ["100", "not_a_number", "300"],
    })
    out = clean_df(df)
    assert len(out) == 2
    assert out["price"].dtype == "float64"


def test_clean_df_drops_duplicates():
    df = pd.DataFrame({
        "product_name": ["A", "A", "B"],
        "category": ["X", "X", "Y"],
        "price": ["100", "100", "200"],
    })
    out = clean_df(df)
    assert len(out) == 2
    assert out["product_name"].tolist() == ["A", "B"]


def test_clean_df_strips_strings():
    df = pd.DataFrame({
        "product_name": ["  A  ", "B"],
        "category": ["  X  ", "Y"],
        "price": ["100", "200"],
    })
    out = clean_df(df)
    assert out["product_name"].iloc[0] == "A"
    assert out["category"].iloc[0] == "X"


def test_clean_df_empty_after_drop():
    df = pd.DataFrame({
        "product_name": [None],
        "category": ["X"],
        "price": [None],
    })
    out = clean_df(df)
    assert len(out) == 0
    assert list(out.columns) == ["product_name", "category", "price"]


# -----------------------------------------------------------------------------
# load_to_pg (mocked)
# -----------------------------------------------------------------------------


def test_load_to_pg_empty_dataframe():
    """Empty DataFrame should return 0 and not call DB."""
    df = pd.DataFrame(columns=["product_name", "category", "price"])
    n = load_to_pg(df, "postgresql://localhost/test")
    assert n == 0


def test_load_to_pg_inserts_rows():
    import unittest.mock
    mock_conn = unittest.mock.MagicMock()
    mock_conn.cursor.return_value.__enter__ = lambda self: self
    mock_conn.cursor.return_value.__exit__ = lambda *a: None
    with unittest.mock.patch("etl.psycopg2.connect", return_value=mock_conn), \
         unittest.mock.patch("etl.execute_values"):
        df = pd.DataFrame({
            "product_name": ["A"],
            "category": ["X"],
            "price": [100.0],
        })
        n = load_to_pg(df, "postgresql://localhost/test")
        assert n == 1
        mock_conn.commit.assert_called_once()


# -----------------------------------------------------------------------------
# run_pipeline (integration: skip if no DB_URI)
# -----------------------------------------------------------------------------


@pytest.mark.skipif(
    not __import__("os").environ.get("DB_URI"),
    reason="DB_URI not set; integration test skipped",
)
def test_run_pipeline_integration():
    """Run full pipeline with sample CSV (requires Postgres and DB_URI)."""
    sample = Path(__file__).resolve().parent.parent / "sample_data" / "products.csv"
    if not sample.exists():
        pytest.skip("sample_data/products.csv not found")
    n = run_pipeline(str(sample))
    assert n >= 0
