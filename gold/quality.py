from pyspark.sql import DataFrame
from pyspark.sql import functions as F


REQUIRED_COLS = ["open", "high", "low", "close", "volume"]


def validate_gold(df: DataFrame, table_name: str) -> None:
    """Validate Gold OHLCV DataFrame before writing to disk.

    Raises ValueError if any quality gate fails.
    """
    missing = [c for c in REQUIRED_COLS + ["vwap"] if c not in df.columns]
    if missing:
        raise ValueError(f"[{table_name}] Missing required columns: {missing}")

    stats = df.agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("open").isNull(), 1).otherwise(0)).alias("null_open"),
        F.sum(F.when(F.col("high").isNull(), 1).otherwise(0)).alias("null_high"),
        F.sum(F.when(F.col("low").isNull(), 1).otherwise(0)).alias("null_low"),
        F.sum(F.when(F.col("close").isNull(), 1).otherwise(0)).alias("null_close"),
        F.sum(F.when(F.col("volume").isNull(), 1).otherwise(0)).alias("null_volume"),
        F.sum(F.when(F.col("high") < F.col("low"), 1).otherwise(0)).alias("invalid_hl"),
        F.sum(F.when(F.col("volume") <= 0, 1).otherwise(0)).alias("non_pos_volume"),
        F.sum(F.when(F.col("open") <= 0, 1).otherwise(0)).alias("non_pos_open"),
        F.sum(F.when(F.col("close") <= 0, 1).otherwise(0)).alias("non_pos_close"),
        F.sum(
            F.when(
                (F.col("vwap") < F.col("low")) | (F.col("vwap") > F.col("high")),
                1,
            ).otherwise(0)
        ).alias("vwap_out_of_bounds"),
    ).collect()[0]

    if stats["row_count"] == 0:
        raise ValueError(f"[{table_name}] Quality gate failed: row_count is 0.")

    null_counts = {
        col_name: stats[f"null_{col_name}"]
        for col_name in REQUIRED_COLS
        if stats[f"null_{col_name}"] and stats[f"null_{col_name}"] > 0
    }
    if null_counts:
        raise ValueError(f"[{table_name}] Quality gate failed: NULL values found: {null_counts}")

    if stats["invalid_hl"] and stats["invalid_hl"] > 0:
        raise ValueError(f"[{table_name}] Quality gate failed: {stats['invalid_hl']} rows have high < low.")

    if stats["non_pos_volume"] and stats["non_pos_volume"] > 0:
        raise ValueError(f"[{table_name}] Quality gate failed: {stats['non_pos_volume']} rows have volume <= 0.")

    if stats["non_pos_open"] and stats["non_pos_open"] > 0:
        raise ValueError(f"[{table_name}] Quality gate failed: {stats['non_pos_open']} rows have open <= 0.")

    if stats["non_pos_close"] and stats["non_pos_close"] > 0:
        raise ValueError(f"[{table_name}] Quality gate failed: {stats['non_pos_close']} rows have close <= 0.")

    if stats["vwap_out_of_bounds"] and stats["vwap_out_of_bounds"] > 0:
        raise ValueError(
            f"[{table_name}] Quality gate failed: {stats['vwap_out_of_bounds']} rows have vwap outside [low, high]."
        )
