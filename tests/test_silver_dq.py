import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.quality.dq_checks import deduplicate_by_key, split_referential_integrity


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("ecommerce-lakehouse-tests")
        .getOrCreate()
    )


def test_deduplicate_by_key_keeps_latest(spark):
    data = [
        ("ord_1", "shipped", "2026-11-15 10:00:00"),
        ("ord_1", "delivered", "2026-11-16 10:00:00"),
        ("ord_2", "created", "2026-11-16 10:00:00"),
    ]
    df = spark.createDataFrame(data, ["order_id", "status", "_ingested_at"])
    result = deduplicate_by_key(df, ["order_id"], [col("_ingested_at").desc()])
    rows = {r["order_id"]: r["status"] for r in result.collect()}
    assert rows == {"ord_1": "delivered", "ord_2": "created"}


def test_split_referential_integrity_catches_orphans(spark):
    orders = spark.createDataFrame([("ord_1",)], ["order_id"])
    items = spark.createDataFrame([("ord_1", 1), ("ord_missing", 1)], ["order_id", "order_item_id"])
    valid_df, orphan_df = split_referential_integrity(items, orders, "order_id")
    assert valid_df.count() == 1
    assert orphan_df.count() == 1
    assert orphan_df.collect()[0]["quarantine_reason"] == "orphan_order_id"
