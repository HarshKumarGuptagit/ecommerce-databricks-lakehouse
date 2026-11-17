from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, dayofmonth, dayofweek, explode, expr, lit,
    month, quarter, sequence, sha2, to_date, year,
)
from src.config import GOLD_SCHEMA, SILVER_SCHEMA
from src.quality.dq_checks import merge_to_delta


def build_dim_products(spark: SparkSession) -> None:
    df = (
        spark.read.table(f"{SILVER_SCHEMA}.silver_products")
        .withColumn("product_sk", sha2(col("product_id"), 256))
    )
    merge_to_delta(spark, df, f"{GOLD_SCHEMA}.dim_products", "target.product_id = source.product_id")


def build_dim_sellers(spark: SparkSession) -> None:
    df = (
        spark.read.table(f"{SILVER_SCHEMA}.silver_sellers")
        .withColumn("seller_sk", sha2(col("seller_id"), 256))
    )
    merge_to_delta(spark, df, f"{GOLD_SCHEMA}.dim_sellers", "target.seller_id = source.seller_id")


def build_dim_date(spark: SparkSession, start_date: str = "2016-01-01", end_date: str = "2019-12-31") -> None:
    date_df = (
        spark.range(1)
        .select(
            explode(
                sequence(to_date(lit(start_date)), to_date(lit(end_date)), expr("interval 1 day"))
            ).alias("date")
        )
        .withColumns({
            "date_key": date_format(col("date"), "yyyyMMdd").cast("int"),
            "full_date": col("date"),
            "year": year(col("date")),
            "quarter": quarter(col("date")),
            "month": month(col("date")),
            "month_name": date_format(col("date"), "MMM"),
            "day_of_month": dayofmonth(col("date")),
            "day_of_week": dayofweek(col("date")),
            "day_name": date_format(col("date"), "EEE"),
            "is_weekend": dayofweek(col("date")).isin([1, 7]),
        })
    )
    (
        date_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_SCHEMA}.dim_date")
    )
