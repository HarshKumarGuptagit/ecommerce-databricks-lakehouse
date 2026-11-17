# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath(".."))

from pyspark.sql import Window
from pyspark.sql.functions import broadcast, col, row_number
from src.config import GOLD_SCHEMA, SILVER_SCHEMA
from src.transformations.scd_handler import init_dim_customers, apply_scd_type2_customers

# COMMAND ----------

dbutils.widgets.text("batch_id", "initial_full_load")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

dim_table = f"{GOLD_SCHEMA}.dim_customers"

if not spark.catalog.tableExists(dim_table):
    init_dim_customers(spark)
else:
    batch_customers = (
        spark.read.table(f"{SILVER_SCHEMA}.silver_customers")
        .filter(col("_batch_id") == batch_id)
    )
    if not batch_customers.isEmpty():
        silver_orders = (
            spark.read.table(f"{SILVER_SCHEMA}.silver_orders")
            .select("customer_id", "order_purchase_timestamp")
        )
        incoming_df = (
            batch_customers
            .join(broadcast(silver_orders), on="customer_id", how="left")
            .withColumn(
                "_rn",
                row_number().over(
                    Window.partitionBy("customer_unique_id")
                    .orderBy(col("order_purchase_timestamp").desc())
                ),
            )
            .filter(col("_rn") == 1)
            .select(
                "customer_unique_id",
                "customer_zip_code_prefix",
                "customer_city",
                "customer_state",
                col("order_purchase_timestamp").alias("effective_from"),
            )
        )
        apply_scd_type2_customers(spark, incoming_df)