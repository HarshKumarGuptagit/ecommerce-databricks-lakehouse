# Databricks notebook source
import sys, os
sys.path.append(os.path.abspath(".."))

from src.transformations.scd_handler import init_dim_customers, apply_scd_type2_customers

# COMMAND ----------

dbutils.widgets.text("batch_id", "initial_full_load")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

init_dim_customers(spark)
