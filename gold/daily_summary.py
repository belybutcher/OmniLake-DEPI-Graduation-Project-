from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def build_daily_summary(
    spark: SparkSession,
    symbol: str,
    target_date: str,
    silver_base_path: str = "data_lake/silver",
) -> DataFrame:
    """Build daily trade summary metrics for a single symbol and date from Silver."""
    silver_path = str(Path(silver_base_path) / symbol)
    df_silver = (
        spark.read.parquet(silver_path)
        .filter(F.col("trade_date") == F.lit(target_date))
    )

    if "quote_quantity" in df_silver.columns:
        quote_vol_expr = F.sum("quote_quantity").cast(DecimalType(28, 8))
    else:
        quote_vol_expr = F.sum(
            F.col("price").cast(DecimalType(38, 12)) * F.col("quantity").cast(DecimalType(38, 12))
        ).cast(DecimalType(28, 8))

    df_daily = (
        df_silver.groupBy("trade_date")
        .agg(
            F.sum("quantity").cast(DecimalType(28, 8)).alias("total_volume"),
            quote_vol_expr.alias("total_quote_volume"),
            F.avg("quantity").cast(DecimalType(28, 8)).alias("avg_trade_size"),
            F.max("quantity").cast(DecimalType(28, 8)).alias("max_trade"),
            F.min("quantity").cast(DecimalType(28, 8)).alias("min_trade"),
            F.count("*").cast("long").alias("trade_count"),
        )
        .withColumn("date", F.col("trade_date").cast("string"))
        .select(
            "date",
            "total_volume",
            "total_quote_volume",
            "avg_trade_size",
            "max_trade",
            "min_trade",
            "trade_count",
        )
    )
    return df_daily


def validate_daily_summary(df: DataFrame, table_name: str) -> None:
    """Validate daily summary DataFrame before writing to Gold."""
    stats = df.agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("total_volume").isNull() | (F.col("total_volume") <= 0), 1).otherwise(0)).alias("bad_vol"),
        F.sum(F.when(F.col("trade_count").isNull() | (F.col("trade_count") <= 0), 1).otherwise(0)).alias("bad_cnt"),
    ).collect()[0]

    if stats["row_count"] == 0:
        raise ValueError(f"[{table_name}] Quality gate failed: row_count is 0.")
    if stats["bad_vol"] and stats["bad_vol"] > 0:
        raise ValueError(f"[{table_name}] Quality gate failed: invalid total_volume found.")
    if stats["bad_cnt"] and stats["bad_cnt"] > 0:
        raise ValueError(f"[{table_name}] Quality gate failed: invalid trade_count found.")


def write_daily_summary(
    df: DataFrame,
    symbol: str,
    gold_base_path: str = "data_lake/gold",
) -> None:
    """Write daily summary idempotently partitioned by date."""
    out_path = str(Path(gold_base_path) / "fct_trades_daily" / symbol)
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .partitionBy("date")
        .parquet(out_path)
    )
