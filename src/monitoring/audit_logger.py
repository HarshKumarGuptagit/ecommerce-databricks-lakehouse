from datetime import datetime
from pyspark.sql import SparkSession
from src.config import AUDIT_TABLE


def log_audit_event(
    spark: SparkSession,
    run_id: str,
    batch_id: str,
    layer: str,
    table_name: str,
    status: str,
    total_rows: int,
    corrupt_rows: int,
    started_at: datetime,
    ended_at: datetime,
    error_message: str = None,
) -> None:
    if not spark.catalog.tableExists(AUDIT_TABLE):
        return
    duration = (ended_at - started_at).total_seconds()
    log_row = [(
        run_id,
        batch_id,
        layer,
        table_name,
        status,
        total_rows,
        corrupt_rows,
        started_at,
        ended_at,
        duration,
        error_message[:1000] if error_message else None,
    )]
    log_schema = spark.table(AUDIT_TABLE).schema
    (
        spark.createDataFrame(log_row, schema=log_schema)
        .write.format("delta")
        .mode("append")
        .saveAsTable(AUDIT_TABLE)
    )
