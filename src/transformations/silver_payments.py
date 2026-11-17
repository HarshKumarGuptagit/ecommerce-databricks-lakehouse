from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, lower, trim, when
from src.config import BRONZE_SCHEMA, SILVER_SCHEMA
from src.quality.dq_checks import (
    deduplicate_by_key,
    merge_to_delta,
    split_referential_integrity,
    write_quarantine_delta,
)


def run_silver_payments(spark: SparkSession, batch_id: str = None) -> None:
    raw_df = spark.read.table(f"{BRONZE_SCHEMA}.bronze_payments")
    if batch_id:
        raw_df = raw_df.filter(col("_batch_id") == batch_id)

    typed_df = raw_df.withColumns({
        "payment_sequential": expr("try_cast(payment_sequential AS INT)"),
        "payment_type": trim(lower(col("payment_type"))),
        "payment_installments": expr("try_cast(payment_installments AS INT)"),
        "payment_value": expr("try_cast(payment_value AS DECIMAL(10, 2))"),
    })

    flagged_df = typed_df.withColumns({
        "invalid_condition": (
            col("order_id").isNull()
            | col("payment_sequential").isNull()
            | col("_corrupt_record").isNotNull()
            | col("payment_type").isNull()
            | col("payment_installments").isNull()
            | (col("payment_installments") < lit(0))
            | col("payment_value").isNull()
            | (col("payment_value") < lit(0))
        ),
        "quarantine_reason": (
            when(col("order_id").isNull(), "OrderId is null")
            .when(col("payment_sequential").isNull(), "Payment sequential is null")
            .when(col("_corrupt_record").isNotNull(), "Corrupt record")
            .when(col("payment_type").isNull(), "Payment Type is null")
            .when(col("payment_installments").isNull() | (col("payment_installments") < lit(0)), "Data quality issue: payment_installments")
            .when(col("payment_value").isNull() | (col("payment_value") < lit(0)), "Data quality issue: payment_value")
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
            key_cols=["order_id", "payment_sequential"],
            order_cols=[col("_ingested_at").desc()],
        )
        .withColumn("is_zero_value_payment", col("payment_value") == 0)
        .drop("invalid_condition", "quarantine_reason", "_corrupt_record")
    )

    write_quarantine_delta(spark, quarantine_df, f"{SILVER_SCHEMA}.quarantine_payments", batch_id)
    merge_to_delta(
        spark,
        clean_df,
        f"{SILVER_SCHEMA}.silver_payments",
        merge_condition="target.order_id = source.order_id AND target.payment_sequential = source.payment_sequential",
        update_condition="source._ingested_at >= target._ingested_at",
    )
