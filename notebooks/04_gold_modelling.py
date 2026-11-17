# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath(".."))

from src.transformations.gold_dimensions import build_dim_products, build_dim_sellers, build_dim_date
from src.transformations.gold_facts import (
    build_fact_orders,
    build_fact_order_items,
    build_fact_payments,
    build_fact_reviews,
)

# COMMAND ----------

dbutils.widgets.text("batch_id", "initial_full_load")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

build_dim_products(spark)
build_dim_sellers(spark)
build_dim_date(spark)

build_fact_orders(spark)
build_fact_order_items(spark)
build_fact_payments(spark)
build_fact_reviews(spark)
