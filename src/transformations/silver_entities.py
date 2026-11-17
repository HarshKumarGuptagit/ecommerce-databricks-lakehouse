from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, expr, lit, lower, trim, upper, when
from src.config import BRONZE_SCHEMA, SILVER_SCHEMA
from src.quality.dq_checks import deduplicate_by_key, merge_to_delta, write_quarantine_delta


def run_silver_customers(spark: SparkSession, batch_id: str = None) -> None:
    raw_df = spark.read.table(f"{BRONZE_SCHEMA}.bronze_customers")
    if batch_id:
        raw_df = raw_df.filter(col("_batch_id") == batch_id)

    typed_df = raw_df.withColumns({
        "customer_zip_code_prefix": trim("customer_zip_code_prefix"),
        "customer_city": trim(lower("customer_city")),
        "customer_state": trim(upper("customer_state")),
    })

    flagged_df = typed_df.withColumns({
        "invalid_condition": (
            col("customer_id").isNull()
            | col("customer_unique_id").isNull()
            | col("_corrupt_record").isNotNull()
        ),
        "quarantine_reason": (
            when(col("customer_id").isNull(), "Customer id is null")
            .when(col("customer_unique_id").isNull(), "Customer Unique id is null")
            .when(col("_corrupt_record").isNotNull(), "Corrupt Record")
            .otherwise("NA")
        ),
    })

    quarantine_df = flagged_df.filter(col("invalid_condition")).drop("invalid_condition")
    valid_df = flagged_df.filter(~col("invalid_condition"))

    clean_df = (
        deduplicate_by_key(valid_df, ["customer_id"], [col("_ingested_at").desc()])
        .drop("invalid_condition", "quarantine_reason", "_corrupt_record")
    )

    write_quarantine_delta(spark, quarantine_df, f"{SILVER_SCHEMA}.quarantine_customers", batch_id)
    merge_to_delta(
        spark,
        clean_df,
        f"{SILVER_SCHEMA}.silver_customers",
        merge_condition="target.customer_id = source.customer_id",
        update_condition="source._ingested_at >= target._ingested_at",
    )


def run_silver_products(spark: SparkSession, batch_id: str = None) -> None:
    raw_df = spark.read.table(f"{BRONZE_SCHEMA}.bronze_products")
    if batch_id:
        raw_df = raw_df.filter(col("_batch_id") == batch_id)

    typed_df = raw_df.withColumns({
        "product_category_name": coalesce(trim(lower("product_category_name")), lit("unknown")),
        "product_name_length": expr("try_cast(product_name_lenght as int)"),
        "product_description_length": expr("try_cast(product_description_lenght as int)"),
        "product_photos_qty": expr("try_cast(product_photos_qty as int)"),
        "product_weight_g": expr("try_cast(product_weight_g as int)"),
        "product_length_cm": expr("try_cast(product_length_cm as int)"),
        "product_height_cm": expr("try_cast(product_height_cm as int)"),
        "product_width_cm": expr("try_cast(product_width_cm as int)"),
    })

    flagged_df = typed_df.withColumns({
        "invalid_condition": col("product_id").isNull() | col("_corrupt_record").isNotNull(),
        "quarantine_reason": (
            when(col("product_id").isNull(), "product id is null")
            .when(col("_corrupt_record").isNotNull(), "Corrupt Record")
            .otherwise("NA")
        ),
    })

    quarantine_df = flagged_df.filter(col("invalid_condition")).drop("invalid_condition")
    valid_df = flagged_df.filter(~col("invalid_condition"))

    clean_df = (
        deduplicate_by_key(valid_df, ["product_id"], [col("_ingested_at").desc()])
        .drop("invalid_condition", "quarantine_reason", "_corrupt_record")
    )

    write_quarantine_delta(spark, quarantine_df, f"{SILVER_SCHEMA}.quarantine_products", batch_id)
    merge_to_delta(
        spark,
        clean_df,
        f"{SILVER_SCHEMA}.silver_products",
        merge_condition="target.product_id = source.product_id",
        update_condition="source._ingested_at >= target._ingested_at",
    )


def run_silver_sellers(spark: SparkSession, batch_id: str = None) -> None:
    raw_df = spark.read.table(f"{BRONZE_SCHEMA}.bronze_sellers")
    if batch_id:
        raw_df = raw_df.filter(col("_batch_id") == batch_id)

    typed_df = raw_df.withColumns({
        "seller_zip_code_prefix": trim("seller_zip_code_prefix").cast("string"),
        "seller_city": trim(lower("seller_city")),
        "seller_state": trim(upper("seller_state")),
    })

    flagged_df = typed_df.withColumns({
        "invalid_condition": col("seller_id").isNull() | col("_corrupt_record").isNotNull(),
        "quarantine_reason": (
            when(col("seller_id").isNull(), "Seller id is null")
            .when(col("_corrupt_record").isNotNull(), "Corrupt Record")
            .otherwise("NA")
        ),
    })

    quarantine_df = flagged_df.filter(col("invalid_condition")).drop("invalid_condition")
    valid_df = flagged_df.filter(~col("invalid_condition"))

    clean_df = (
        deduplicate_by_key(valid_df, ["seller_id"], [col("_ingested_at").desc()])
        .drop("invalid_condition", "quarantine_reason", "_corrupt_record")
    )

    write_quarantine_delta(spark, quarantine_df, f"{SILVER_SCHEMA}.quarantine_sellers", batch_id)
    merge_to_delta(
        spark,
        clean_df,
        f"{SILVER_SCHEMA}.silver_sellers",
        merge_condition="target.seller_id = source.seller_id",
        update_condition="source._ingested_at >= target._ingested_at",
    )
