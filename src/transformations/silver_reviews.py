from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, trim, try_to_timestamp, when
from src.config import BRONZE_SCHEMA, SILVER_SCHEMA
from src.quality.dq_checks import (
    deduplicate_by_key,
    merge_to_delta,
    split_referential_integrity,
    write_quarantine_delta,
)


def run_silver_reviews(spark: SparkSession, batch_id: str = None) -> None:
    raw_df = spark.read.table(f"{BRONZE_SCHEMA}.bronze_reviews")
    if batch_id:
        raw_df = raw_df.filter(col("_batch_id") == batch_id)

    typed_df = raw_df.withColumns({
        "review_score": expr("try_cast(review_score AS INT)"),
        "review_creation_date": try_to_timestamp("review_creation_date"),
        "review_answer_timestamp": try_to_timestamp("review_answer_timestamp"),
        "review_comment_title": trim(col("review_comment_title")),
        "review_comment_message": trim(col("review_comment_message")),
    })

    flagged_df = typed_df.withColumns({
        "invalid_condition": (
            col("review_id").isNull()
            | col("order_id").isNull()
            | col("_corrupt_record").isNotNull()
            | col("review_creation_date").isNull()
            | col("review_score").isNull()
            | ~col("review_score").between(1, 5)
        ),
        "quarantine_reason": (
            when(col("review_id").isNull(), "review_id is null")
            .when(col("order_id").isNull(), "order_id is null")
            .when(col("_corrupt_record").isNotNull(), "Corrupt record")
            .when(col("review_creation_date").isNull(), "review_creation_date is null")
            .when(col("review_score").isNull(), "review_score is null")
            .when(~col("review_score").between(1, 5), "Review Score Outside Range")
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
            key_cols=["review_id", "order_id"],
            order_cols=[col("_ingested_at").desc()],
        )
        .drop("invalid_condition", "quarantine_reason", "_corrupt_record")
    )

    write_quarantine_delta(spark, quarantine_df, f"{SILVER_SCHEMA}.quarantine_reviews", batch_id)
    merge_to_delta(
        spark,
        clean_df,
        f"{SILVER_SCHEMA}.silver_reviews",
        merge_condition="target.order_id = source.order_id AND target.review_id = source.review_id",
        update_condition="source._ingested_at >= target._ingested_at",
    )
