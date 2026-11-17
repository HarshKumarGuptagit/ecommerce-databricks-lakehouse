# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath(".."))

from src.transformations.silver_orders import run_silver_orders
from src.transformations.silver_order_items import run_silver_order_items
from src.transformations.silver_payments import run_silver_payments
from src.transformations.silver_reviews import run_silver_reviews
from src.transformations.silver_entities import (
    run_silver_customers,
    run_silver_products,
    run_silver_sellers,
)

# COMMAND ----------

dbutils.widgets.text("batch_id", "initial_full_load")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

run_silver_orders(spark, batch_id=batch_id)
run_silver_order_items(spark, batch_id=batch_id)
run_silver_payments(spark, batch_id=batch_id)
run_silver_reviews(spark, batch_id=batch_id)
run_silver_customers(spark, batch_id=batch_id)
run_silver_products(spark, batch_id=batch_id)
run_silver_sellers(spark, batch_id=batch_id)
