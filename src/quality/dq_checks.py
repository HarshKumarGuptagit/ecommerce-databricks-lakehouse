from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import broadcast, col, lit, row_number


def deduplicate_by_key(
    df: DataFrame,
    key_cols: list[str],
    order_cols: list,
) -> DataFrame:
    window_spec = Window.partitionBy(*key_cols).orderBy(*order_cols)
    return (
        df.withColumn("_rn", row_number().over(window_spec))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )


def split_referential_integrity(
    df: DataFrame,
    parent_keys_df: DataFrame,
    join_col: str,
    reason: str = "orphan_order_id",
) -> tuple[DataFrame, DataFrame]:
    orphans_df = (
        df.join(broadcast(parent_keys_df), on=join_col, how="left_anti")
        .withColumn("quarantine_reason", lit(reason))
    )
    valid_df = df.join(broadcast(parent_keys_df), on=join_col, how="left_semi")
    return valid_df, orphans_df


def write_quarantine_delta(
    spark: SparkSession,
    quarantine_df: DataFrame,
    quarantine_table: str,
    batch_id: str = None,
) -> None:
    if not spark.catalog.tableExists(quarantine_table) or not spark.catalog.listColumns(quarantine_table):
        (
            quarantine_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(quarantine_table)
        )
    elif batch_id:
        (
            quarantine_df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_batch_id = '{batch_id}'")
            .saveAsTable(quarantine_table)
        )
    elif not quarantine_df.isEmpty():
        quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)


def merge_to_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    merge_condition: str,
    update_condition: str = None,
) -> None:
    if not spark.catalog.tableExists(target_table) or not spark.catalog.listColumns(target_table):
        (
            source_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
    elif not source_df.isEmpty():
        merger = (
            DeltaTable.forName(spark, target_table)
            .alias("target")
            .merge(source_df.alias("source"), merge_condition)
        )
        if update_condition:
            merger = merger.whenMatchedUpdateAll(condition=update_condition)
        else:
            merger = merger.whenMatchedUpdateAll()
        merger.whenNotMatchedInsertAll().execute()
