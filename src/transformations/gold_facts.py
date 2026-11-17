from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast, coalesce, col, count as spark_count, date_format,
    datediff, lit, round as spark_round, sum as spark_sum, unix_timestamp, when,
)
from src.config import GOLD_SCHEMA, SILVER_SCHEMA
from src.quality.dq_checks import merge_to_delta


def build_fact_orders(spark: SparkSession) -> None:
    silver_orders = spark.read.table(f"{SILVER_SCHEMA}.silver_orders")
    silver_order_items = spark.read.table(f"{SILVER_SCHEMA}.silver_order_items")
    silver_payments = spark.read.table(f"{SILVER_SCHEMA}.silver_payments")
    silver_customers = spark.read.table(f"{SILVER_SCHEMA}.silver_customers").select("customer_id", "customer_unique_id")
    dim_customers = (
        spark.read.table(f"{GOLD_SCHEMA}.dim_customers")
        .filter(col("is_current") == True)
        .select("customer_unique_id", "customer_sk")
    )

    items_agg = (
        silver_order_items.groupBy("order_id")
        .agg(
            spark_count("*").alias("total_items_count"),
            spark_sum("price").cast("decimal(12, 2)").alias("total_item_price"),
            spark_sum("freight_value").cast("decimal(12, 2)").alias("total_freight_value"),
            spark_sum("total_item_value").cast("decimal(12, 2)").alias("total_order_value"),
        )
    )

    payments_agg = (
        silver_payments.groupBy("order_id")
        .agg(
            spark_sum("payment_value").cast("decimal(12, 2)").alias("total_payment_value"),
            spark_count("*").alias("payment_count"),
        )
    )

    fact_orders_df = (
        silver_orders
        .join(items_agg, on="order_id", how="left")
        .join(payments_agg, on="order_id", how="left")
        .join(broadcast(silver_customers), on="customer_id", how="left")
        .join(broadcast(dim_customers), on="customer_unique_id", how="left")
        .withColumns({
            "order_date_key": date_format(col("order_purchase_timestamp"), "yyyyMMdd").cast("int"),
            "total_items_count": coalesce(col("total_items_count"), lit(0)),
            "total_item_price": coalesce(col("total_item_price"), lit(0).cast("decimal(12, 2)")),
            "total_freight_value": coalesce(col("total_freight_value"), lit(0).cast("decimal(12, 2)")),
            "total_order_value": coalesce(col("total_order_value"), lit(0).cast("decimal(12, 2)")),
            "total_payment_value": coalesce(col("total_payment_value"), lit(0).cast("decimal(12, 2)")),
            "payment_count": coalesce(col("payment_count"), lit(0)),
            "delivery_days": datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")),
            "estimated_delivery_days": datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp")),
            "is_late_delivery": when(
                (col("order_status") == "delivered")
                & (col("order_delivered_customer_date") > col("order_estimated_delivery_date")),
                True,
            ).otherwise(False),
        })
        .select(
            "order_id", "customer_sk", "customer_unique_id", "order_date_key", "order_status",
            "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
            "order_delivered_customer_date", "order_estimated_delivery_date",
            "total_items_count", "total_item_price", "total_freight_value", "total_order_value",
            "total_payment_value", "payment_count", "delivery_days", "estimated_delivery_days",
            "is_late_delivery", "has_missing_delivered_date",
        )
    )
    merge_to_delta(spark, fact_orders_df, f"{GOLD_SCHEMA}.fact_orders", "target.order_id = source.order_id")


def build_fact_order_items(spark: SparkSession) -> None:
    silver_items = spark.read.table(f"{SILVER_SCHEMA}.silver_order_items")
    dim_products = spark.read.table(f"{GOLD_SCHEMA}.dim_products").select("product_id", "product_sk")
    dim_sellers = spark.read.table(f"{GOLD_SCHEMA}.dim_sellers").select("seller_id", "seller_sk")
    fact_orders = spark.read.table(f"{GOLD_SCHEMA}.fact_orders").select(
        "order_id", "customer_sk", "order_date_key", "order_status"
    )

    fact_df = (
        silver_items
        .join(broadcast(dim_products), on="product_id", how="left")
        .join(broadcast(dim_sellers), on="seller_id", how="left")
        .join(fact_orders, on="order_id", how="left")
        .select(
            "order_id", "order_item_id", "product_sk", "product_id",
            "seller_sk", "seller_id", "customer_sk", "order_date_key", "order_status",
            "shipping_limit_date", "price", "freight_value", "total_item_value",
        )
    )
    merge_to_delta(
        spark, fact_df, f"{GOLD_SCHEMA}.fact_order_items",
        "target.order_id = source.order_id AND target.order_item_id = source.order_item_id",
    )


def build_fact_payments(spark: SparkSession) -> None:
    silver_payments = spark.read.table(f"{SILVER_SCHEMA}.silver_payments")
    fact_orders = spark.read.table(f"{GOLD_SCHEMA}.fact_orders").select(
        "order_id", "customer_sk", "order_date_key", "order_status"
    )

    fact_df = (
        silver_payments
        .join(fact_orders, on="order_id", how="left")
        .select(
            "order_id", "payment_sequential", "customer_sk", "order_date_key", "order_status",
            "payment_type", "payment_installments", "payment_value", "is_zero_value_payment",
        )
    )
    merge_to_delta(
        spark, fact_df, f"{GOLD_SCHEMA}.fact_payments",
        "target.order_id = source.order_id AND target.payment_sequential = source.payment_sequential",
    )


def build_fact_reviews(spark: SparkSession) -> None:
    silver_reviews = spark.read.table(f"{SILVER_SCHEMA}.silver_reviews")
    fact_orders = spark.read.table(f"{GOLD_SCHEMA}.fact_orders").select(
        "order_id", "customer_sk", "order_date_key", "order_status"
    )

    fact_df = (
        silver_reviews
        .join(fact_orders, on="order_id", how="left")
        .withColumn(
            "review_response_hours",
            spark_round(
                (unix_timestamp(col("review_answer_timestamp")) - unix_timestamp(col("review_creation_date"))) / 3600.0,
                2,
            ),
        )
        .select(
            "review_id", "order_id", "customer_sk", "order_date_key", "order_status",
            "review_score", "review_comment_title", "review_comment_message",
            "review_creation_date", "review_answer_timestamp", "review_response_hours",
        )
    )
    merge_to_delta(
        spark, fact_df, f"{GOLD_SCHEMA}.fact_reviews",
        "target.review_id = source.review_id AND target.order_id = source.order_id",
    )
