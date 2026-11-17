from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp, lit
from pyspark.sql.types import StringType, StructField, StructType
from src.config import BRONZE_SCHEMA, RAW_SCHEMAS, SOURCE_VOLUME_PATH
from src.monitoring.audit_logger import log_audit_event


def build_bronze_schema(columns: list[str]) -> StructType:
    fields = [StructField(c, StringType(), True) for c in columns]
    fields.append(StructField("_corrupt_record", StringType(), True))
    return StructType(fields)


def ingest_to_bronze(
    spark: SparkSession,
    dataset_name: str,
    file_name: str,
    batch_id: str,
    run_id: str,
) -> dict:
    source_file_path = f"{SOURCE_VOLUME_PATH}{file_name}"
    target_table = f"{BRONZE_SCHEMA}.bronze_{dataset_name}"
    started_at = datetime.now(timezone.utc)

    try:
        schema = build_bronze_schema(RAW_SCHEMAS[dataset_name])
        df = (
            spark.read.format("csv")
            .schema(schema)
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("multiLine", True)
            .option("escape", '"')
            .load(source_file_path)
        )

        bronze_df = (
            df.withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
            .withColumn("_batch_id", lit(batch_id))
        )

        writer = bronze_df.write.format("delta").mode("overwrite")
        if spark.catalog.tableExists(target_table):
            writer = writer.option("replaceWhere", f"_batch_id = '{batch_id}'")
        else:
            writer = writer.option("overwriteSchema", "true")
        writer.saveAsTable(target_table)

        metrics = (
            spark.table(target_table)
            .filter(col("_batch_id") == batch_id)
            .agg(
                count("*").alias("total_rows"),
                count("_corrupt_record").alias("corrupt_rows"),
            )
            .collect()[0]
        )

        ended_at = datetime.now(timezone.utc)
        log_audit_event(
            spark, run_id, batch_id, "bronze", target_table, "SUCCESS",
            metrics["total_rows"], metrics["corrupt_rows"], started_at, ended_at,
        )
        return {
            "table": target_table,
            "batch_id": batch_id,
            "total_rows": metrics["total_rows"],
            "corrupt_rows": metrics["corrupt_rows"],
        }
    except Exception as exc:
        ended_at = datetime.now(timezone.utc)
        log_audit_event(
            spark, run_id, batch_id, "bronze", target_table, "FAILED",
            0, 0, started_at, ended_at, str(exc),
        )
        raise
