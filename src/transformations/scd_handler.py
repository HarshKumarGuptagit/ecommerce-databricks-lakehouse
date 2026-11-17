from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import broadcast, col, concat_ws, lit, row_number, sha2
from src.config import GOLD_SCHEMA, SILVER_SCHEMA

DIM_CUSTOMERS_TABLE = f"{GOLD_SCHEMA}.dim_customers"


def init_dim_customers(spark: SparkSession) -> None:
    silver_customers = spark.read.table(f"{SILVER_SCHEMA}.silver_customers")
    silver_orders = (
        spark.read.table(f"{SILVER_SCHEMA}.silver_orders")
        .select("customer_id", "order_purchase_timestamp")
    )

    latest_customers = (
        silver_customers
        .join(broadcast(silver_orders), on="customer_id", how="left")
        .withColumn(
            "row_number",
            row_number().over(
                Window.partitionBy("customer_unique_id")
                .orderBy(col("order_purchase_timestamp").desc())
            ),
        )
        .filter(col("row_number") == 1)
        .drop("row_number")
    )

    dim_df = (
        latest_customers
        .withColumns({
            "effective_from": col("order_purchase_timestamp"),
            "effective_to": lit(None).cast("timestamp"),
            "is_current": lit(True),
            "row_hash": sha2(
                concat_ws("||", col("customer_zip_code_prefix"), col("customer_city"), col("customer_state")),
                256,
            ),
            "customer_sk": sha2(
                concat_ws("||", col("customer_unique_id"), col("order_purchase_timestamp").cast("string")),
                256,
            ),
        })
        .select(
            "customer_sk", "customer_unique_id", "customer_zip_code_prefix",
            "customer_city", "customer_state", "row_hash",
            "effective_from", "effective_to", "is_current",
        )
    )

    if not spark.catalog.tableExists(DIM_CUSTOMERS_TABLE):
        dim_df.write.format("delta").mode("overwrite").saveAsTable(DIM_CUSTOMERS_TABLE)


def apply_scd_type2_customers(spark: SparkSession, incoming_df: DataFrame) -> None:
    staged_source = incoming_df.withColumns({
        "row_hash": sha2(
            concat_ws("||", col("customer_zip_code_prefix"), col("customer_city"), col("customer_state")),
            256,
        ),
        "customer_sk": sha2(
            concat_ws("||", col("customer_unique_id"), col("effective_from").cast("string")),
            256,
        ),
        "effective_to": lit(None).cast("timestamp"),
        "is_current": lit(True),
    })

    current_dim = (
        spark.read.table(DIM_CUSTOMERS_TABLE)
        .filter(col("is_current") == True)
        .select(col("customer_unique_id").alias("t_id"), col("row_hash").alias("t_hash"))
    )

    joined = staged_source.join(
        current_dim,
        staged_source.customer_unique_id == current_dim.t_id,
        how="left",
    )

    dim_cols = [
        "customer_sk", "customer_unique_id", "customer_zip_code_prefix",
        "customer_city", "customer_state", "row_hash",
        "effective_from", "effective_to", "is_current",
    ]

    rows_to_expire = (
        joined
        .filter(col("t_id").isNotNull() & (col("row_hash") != col("t_hash")))
        .select(col("customer_unique_id").alias("merge_key"), *dim_cols)
    )

    rows_to_insert = (
        joined
        .filter(col("t_id").isNull() | (col("row_hash") != col("t_hash")))
        .select(lit(None).cast("string").alias("merge_key"), *dim_cols)
    )

    staged_updates = rows_to_expire.unionByName(rows_to_insert)

    if not staged_updates.isEmpty():
        (
            DeltaTable.forName(spark, DIM_CUSTOMERS_TABLE).alias("target")
            .merge(
                staged_updates.alias("source"),
                "target.customer_unique_id = source.merge_key AND target.is_current = true",
            )
            .whenMatchedUpdate(set={
                "is_current": lit(False),
                "effective_to": col("source.effective_from"),
            })
            .whenNotMatchedInsert(values={c: col(f"source.{c}") for c in dim_cols})
            .execute()
        )
