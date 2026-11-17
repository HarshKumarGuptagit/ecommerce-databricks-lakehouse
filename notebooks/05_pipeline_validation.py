# Databricks notebook source
from pyspark.sql.functions import col, sum as spark_sum

dbutils.widgets.text("batch_id", "initial_full_load")
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

silver_orders_cnt = spark.read.table("workspace.ecommerce_silver.silver_orders").count()
fact_orders_cnt = spark.read.table("workspace.ecommerce_gold.fact_orders").count()
assert silver_orders_cnt == fact_orders_cnt, f"Row mismatch: {silver_orders_cnt} != {fact_orders_cnt}"

silver_rev = spark.read.table("workspace.ecommerce_silver.silver_order_items").agg(spark_sum("price")).collect()[0][0]
gold_rev = spark.read.table("workspace.ecommerce_gold.fact_orders").agg(spark_sum("total_item_price")).collect()[0][0]
assert silver_rev == gold_rev, f"Revenue mismatch: {silver_rev} != {gold_rev}"

dup_orders = (
    spark.read.table("workspace.ecommerce_gold.fact_orders")
    .groupBy("order_id").count().filter(col("count") > 1).count()
)
assert dup_orders == 0, f"Found {dup_orders} duplicate order_ids in fact_orders"

dup_active_cust = (
    spark.read.table("workspace.ecommerce_gold.dim_customers")
    .filter(col("is_current") == True)
    .groupBy("customer_unique_id").count().filter(col("count") > 1).count()
)
assert dup_active_cust == 0, f"Found {dup_active_cust} customers with >1 active SCD2 row"

orphan_items = (
    spark.read.table("workspace.ecommerce_gold.fact_order_items")
    .join(spark.read.table("workspace.ecommerce_gold.fact_orders").select("order_id"), on="order_id", how="left_anti")
    .count()
)
assert orphan_items == 0, f"Found {orphan_items} orphan items in fact_order_items"

print("ALL VALIDATION CHECKS PASSED!")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE workspace.ecommerce_gold.fact_orders;
# MAGIC OPTIMIZE workspace.ecommerce_gold.fact_order_items;
# MAGIC ANALYZE TABLE workspace.ecommerce_gold.fact_orders COMPUTE STATISTICS FOR ALL COLUMNS;
