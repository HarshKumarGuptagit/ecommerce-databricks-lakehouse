from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, try_to_timestamp, when
from src.config import BRONZE_SCHEMA, SILVER_SCHEMA
from src.quality.dq_checks import deduplicate_by_key, merge_to_delta, write_quarantine_delta


def run_silver_orders(spark: SparkSession, batch_id: str = None) -> None:
    orders_df = spark.read.table(f"{BRONZE_SCHEMA}.bronze_orders")
    if batch_id:
        orders_df = orders_df.filter(col("_batch_id") == batch_id)

    orders_df = orders_df.withColumns({
        "order_purchase_timestamp": try_to_timestamp(col("order_purchase_timestamp")),
        "order_approved_at": try_to_timestamp(col("order_approved_at")),
        "order_delivered_carrier_date": try_to_timestamp(col("order_delivered_carrier_date")),
        "order_delivered_customer_date": try_to_timestamp(col("order_delivered_customer_date")),
        "order_estimated_delivery_date": try_to_timestamp(col("order_estimated_delivery_date")),
        "order_status": lower(trim(col("order_status"))),
    })

    orders_df = orders_df.withColumns({
        "invalid_condition": (
            col("order_id").isNull()
            | col("customer_id").isNull()
            | col("_corrupt_record").isNotNull()
            | col("order_purchase_timestamp").isNull()
        ),
        "quarantine_reason": (
            when(col("order_id").isNull(), "Order ID is null")
            .when(col("customer_id").isNull(), "Customer ID is null")
            .when(col("_corrupt_record").isNotNull(), "Corrupt record")
            .when(col("order_purchase_timestamp").isNull(), "Purchase timestamp is null")
            .otherwise("NA")
        ),
    })

    quarantine_df = orders_df.filter(col("invalid_condition")).drop("invalid_condition")
    valid_df = orders_df.filter(~col("invalid_condition"))

    clean_df = (
        deduplicate_by_key(
            valid_df,
            key_cols=["order_id"],
            order_cols=[col("_ingested_at").desc(), col("order_delivered_customer_date").desc_nulls_last()],
        )
        .drop("invalid_condition", "quarantine_reason", "_corrupt_record")
        .withColumn(
            "has_missing_delivered_date",
            (col("order_status") == "delivered") & col("order_delivered_customer_date").isNull(),
        )
    )

    write_quarantine_delta(spark, quarantine_df, f"{SILVER_SCHEMA}.quarantine_orders", batch_id)
    merge_to_delta(
        spark,
        clean_df,
        f"{SILVER_SCHEMA}.silver_orders",
        merge_condition="target.order_id = source.order_id",
        update_condition="source._ingested_at >= target._ingested_at",
    )
