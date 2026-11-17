from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, try_to_timestamp, when
from src.config import BRONZE_SCHEMA, SILVER_SCHEMA
from src.quality.dq_checks import (
    deduplicate_by_key,
    merge_to_delta,
    split_referential_integrity,
    write_quarantine_delta,
)


def run_silver_order_items(spark: SparkSession, batch_id: str = None) -> None:
    raw_df = spark.read.table(f"{BRONZE_SCHEMA}.bronze_order_items")
    if batch_id:
        raw_df = raw_df.filter(col("_batch_id") == batch_id)

    typed_df = raw_df.withColumns({
        "order_item_id": expr("try_cast(order_item_id AS INT)"),
        "shipping_limit_date": try_to_timestamp(col("shipping_limit_date")),
        "price": expr("try_cast(price AS DECIMAL(10, 2))"),
        "freight_value": expr("try_cast(freight_value AS DECIMAL(10, 2))"),
    })

    flagged_df = typed_df.withColumns({
        "total_item_value": col("price") + col("freight_value"),
        "invalid_condition": (
            col("order_id").isNull()
            | col("order_item_id").isNull()
            | col("product_id").isNull()
            | col("seller_id").isNull()
            | col("_corrupt_record").isNotNull()
            | col("shipping_limit_date").isNull()
            | col("price").isNull()
            | (col("price") <= lit(0))
            | col("freight_value").isNull()
            | (col("freight_value") < lit(0))
        ),
        "quarantine_reason": (
            when(col("order_id").isNull(), "OrderId is null")
            .when(col("order_item_id").isNull(), "OrderItemId is null")
            .when(col("product_id").isNull(), "product_id is null")
            .when(col("seller_id").isNull(), "seller_id is null")
            .when(col("_corrupt_record").isNotNull(), "Corrupt record")
            .when(col("shipping_limit_date").isNull(), "shipping_limit_date is null")
            .when(col("price").isNull() | (col("price") <= lit(0)), "Data quality issue: Price")
            .when(col("freight_value").isNull() | (col("freight_value") < lit(0)), "Data quality issue: Freight Value")
            .otherwise("NA")
        ),
    })

    dq_quarantine_df = flagged_df.filter(col("invalid_condition"))
    dq_valid_df = flagged_df.filter(~col("invalid_condition"))

    existing_order_ids = spark.read.table(f"{SILVER_SCHEMA}.silver_orders").select("order_id")
    ri_valid_df, orphan_df = split_referential_integrity(dq_valid_df, existing_order_ids, "order_id")

    quarantine_df = dq_quarantine_df.unionByName(orphan_df).drop("invalid_condition")
    clean_df = (
        deduplicate_by_key(
            ri_valid_df,
            key_cols=["order_id", "order_item_id"],
            order_cols=[col("_ingested_at").desc()],
        )
        .drop("invalid_condition", "quarantine_reason", "_corrupt_record")
    )

    write_quarantine_delta(spark, quarantine_df, f"{SILVER_SCHEMA}.quarantine_order_items", batch_id)
    merge_to_delta(
        spark,
        clean_df,
        f"{SILVER_SCHEMA}.silver_order_items",
        merge_condition="target.order_id = source.order_id AND target.order_item_id = source.order_item_id",
        update_condition="source._ingested_at >= target._ingested_at",
    )
