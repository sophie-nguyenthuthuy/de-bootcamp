#!/usr/bin/env python3
"""
Tạo file Parquet mẫu cho Feast: data/customer_features.parquet
Cột: customer_id, total_amount, order_count, event_timestamp
Chạy từ thư mục feast_repo: python scripts/generate_parquet.py
"""
import os
from datetime import datetime, timezone

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(REPO_ROOT, "data")
OUT_PATH = os.path.join(DATA_DIR, "customer_features.parquet")

os.makedirs(DATA_DIR, exist_ok=True)

df = pd.DataFrame({
    "customer_id": [1, 2, 3],
    "total_amount": [350.0, 200.0, 150.0],
    "order_count": [3, 2, 1],
    "event_timestamp": [
        datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        datetime(2025, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
        datetime(2025, 1, 3, 12, 0, 0, tzinfo=timezone.utc),
    ],
})
df.to_parquet(OUT_PATH, index=False)
print(f"Created {OUT_PATH}")
