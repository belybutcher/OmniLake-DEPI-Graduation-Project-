from pathlib import Path
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

INTERVAL_TO_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


def _bucket_start_expr(ts_col: F.Column, interval: str) -> F.Column:
    """Align timestamp to exact interval boundary in UTC."""
    if interval not in INTERVAL_TO_SECONDS:
        raise ValueError(f"Unsupported interval: {interval}")
    if interval == "1d":
        return F.date_trunc("day", ts_col)
    seconds = INTERVAL_TO_SECONDS[interval]
    epoch_sec = F.floor(F.unix_timestamp(ts_col)).cast("long")
    bucket_epoch = (F.floor(epoch_sec / F.lit(seconds)) * F.lit(seconds)).cast("long")
    return F.from_unixtime(bucket_epoch).cast("timestamp")


def build_ohlcv(
    spark: SparkSession,
    symbol: str,
    interval: str,
    target_date: str,
    silver_base_path: str = "data_lake/silver",
) -> DataFrame:
    """Build OHLCV + VWAP bars for a single symbol, interval, and date from Silver."""
    if interval not in INTERVAL_TO_SECONDS:
        raise ValueError(f"Unsupported interval: {interval}")

    silver_path = str(Path(silver_base_path) / symbol)
    df_silver = (
        spark.read.parquet(silver_path)
        .filter(F.col("trade_date") == F.lit(target_date))
    )

    if "trade_timestamp" in df_silver.columns:
        ts_col = F.col("trade_timestamp").cast("timestamp")
    elif "timestamp_ms" in df_silver.columns:
        ts_col = (F.col("timestamp_ms").cast("double") / F.lit(1000.0)).cast("timestamp")
    else:
        raise ValueError("Silver DataFrame must contain 'trade_timestamp' or 'timestamp_ms'.")

    df_with_ts = (
        df_silver.withColumn("_ts", ts_col)
        .withColumn("_order_ts", F.unix_micros(F.col("_ts")))
        .withColumn("bucket_start", _bucket_start_expr(F.col("_ts"), interval))
    )

    asc_cols = [F.col("_order_ts").asc()]
    desc_cols = [F.col("_order_ts").desc()]
    if "trade_id" in df_with_ts.columns:
        asc_cols.append(F.col("trade_id").asc())
        desc_cols.append(F.col("trade_id").desc())

    w_asc = Window.partitionBy("bucket_start").orderBy(*asc_cols).rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    w_desc = (
        Window.partitionBy("bucket_start")
        .orderBy(*desc_cols)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df_with_bounds = (
        df_with_ts.withColumn("open_raw", F.first(F.col("price"), ignorenulls=True).over(w_asc))
        .withColumn("close_raw", F.first(F.col("price"), ignorenulls=True).over(w_desc))
        .withColumn(
            "notional_raw",
            F.col("price").cast(DecimalType(38, 12)) * F.col("quantity").cast(DecimalType(38, 12)),
        )
    )

    df_agg = (
        df_with_bounds.groupBy("bucket_start")
        .agg(
            F.first("open_raw").cast(DecimalType(18, 8)).alias("open"),
            F.max("price").cast(DecimalType(18, 8)).alias("high"),
            F.min("price").cast(DecimalType(18, 8)).alias("low"),
            F.first("close_raw").cast(DecimalType(18, 8)).alias("close"),
            F.sum("quantity").cast(DecimalType(28, 8)).alias("volume"),
            (
                F.sum("notional_raw")
                / F.nullif(F.sum("quantity").cast(DecimalType(38, 12)), F.lit(0))
            )
            .cast(DecimalType(18, 8))
            .alias("vwap"),
            F.count("*").cast("long").alias("trade_count"),
        )
        .withColumn("date", F.date_format(F.col("bucket_start"), "yyyy-MM-dd"))
        .select(
            "bucket_start",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vwap",
            "trade_count",
            "date",
        )
        .orderBy("bucket_start")
    )

    return df_agg


def write_ohlcv(
    df: DataFrame,
    symbol: str,
    interval: str,
    gold_base_path: str = "data_lake/gold",
    num_partitions: int = 1,
) -> None:
    """Write validated OHLCV DataFrame idempotently partitioned by date."""
    out_path = str(Path(gold_base_path) / f"fct_ohlcv_{interval}" / symbol)
    writer = df.coalesce(num_partitions) if num_partitions == 1 else df.repartition(num_partitions)
    (
        writer.write.mode("overwrite")
        .partitionBy("date")
        .parquet(out_path)
    )
