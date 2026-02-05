"""
Week 9 Buổi 17 — Feast feature repo: Entity + FeatureView (FileSource Parquet).
Chạy từ thư mục feast_repo: feast apply
Cần: data/customer_features.parquet (chạy python scripts/generate_parquet.py để tạo).
"""
from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int64

# Entity: customer
customer = Entity(
    name="customer_id",
    join_keys=["customer_id"],
)

# Data source: Parquet (tạo bằng scripts/generate_parquet.py)
customer_features_source = FileSource(
    path="data/customer_features.parquet",
    timestamp_field="event_timestamp",
)

# Feature view: tổng hợp theo customer (total_amount, order_count)
customer_features_view = FeatureView(
    name="customer_features",
    entities=[customer],
    ttl=timedelta(days=1),
    schema=[
        Field(name="total_amount", dtype=Float32),
        Field(name="order_count", dtype=Int64),
    ],
    source=customer_features_source,
)
