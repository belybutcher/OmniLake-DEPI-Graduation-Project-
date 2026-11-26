from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import yaml


def load_symbols_from_config(config_path: str = "config/symbols.yaml") -> list[str]:
    """Load configured symbol names from YAML configuration."""
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    items = data.get("symbols", [])
    return [str(item["symbol"]) for item in items if isinstance(item, dict) and "symbol" in item]


def build_dim_symbol(
    spark: SparkSession,
    config_path: str = "config/symbols.yaml",
) -> DataFrame:
    """Build dim_symbol DataFrame from YAML configuration."""
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    rows = []
    for item in data.get("symbols", []):
        if isinstance(item, dict) and "symbol" in item:
            rows.append(
                (
                    str(item["symbol"]),
                    str(item.get("base_asset", "")),
                    str(item.get("quote_asset", "")),
                    str(item.get("market_type", "spot")),
                )
            )

    if not rows:
        raise ValueError(f"[dim_symbol] No symbols found in {config_path}.")

    schema = "symbol string, base_asset string, quote_asset string, market_type string"
    return spark.createDataFrame(rows, schema=schema).dropDuplicates(["symbol"])


def build_dim_time(
    spark: SparkSession,
    silver_base_path: str = "data_lake/silver",
    config_path: str = "config/symbols.yaml",
) -> DataFrame:
    """Build dim_time from distinct trade_date values available across Silver symbol directories."""
    base_dir = Path(silver_base_path)
    candidate_symbols = load_symbols_from_config(config_path)

    symbol_dirs: list[Path] = []
    for sym in candidate_symbols:
        p = base_dir / sym
        if p.exists() and p.is_dir():
            symbol_dirs.append(p)

    # Fallback to any symbol directory under silver_base_path if not in config
    if not symbol_dirs and base_dir.exists():
        for p in sorted(base_dir.iterdir()):
            if p.is_dir() and not p.name.startswith("."):
                symbol_dirs.append(p)

    date_dfs: list[DataFrame] = []
    for p in symbol_dirs:
        df_sym = spark.read.parquet(str(p))
        if "trade_date" in df_sym.columns:
            date_dfs.append(
                df_sym.select(F.col("trade_date").cast("date").alias("date"))
                .where(F.col("date").isNotNull())
                .dropDuplicates(["date"])
            )

    if not date_dfs:
        raise ValueError("[dim_time] No valid Silver symbol partitions with 'trade_date' found.")

    df_dates = date_dfs[0]
    for extra_df in date_dfs[1:]:
        df_dates = df_dates.unionByName(extra_df).dropDuplicates(["date"])

    df_dim_time = (
        df_dates.withColumn("hour", F.lit(0).cast("int"))
        .withColumn("minute", F.lit(0).cast("int"))
        .withColumn("month", F.month("date").cast("int"))
        .withColumn("year", F.year("date").cast("int"))
        .withColumn(
            "is_weekend",
            F.dayofweek("date").isin([1, 7]).cast("boolean"),
        )
        .withColumn(
            "is_month_end",
            (F.col("date") == F.last_day("date")).cast("boolean"),
        )
        .select(
            "date",
            "hour",
            "minute",
            "month",
            "year",
            "is_weekend",
            "is_month_end",
        )
        .orderBy("date")
    )
    return df_dim_time


def write_dimensions(
    spark: SparkSession,
    gold_base_path: str = "data_lake/gold",
    silver_base_path: str = "data_lake/silver",
    config_path: str = "config/symbols.yaml",
) -> None:
    """Write dim_symbol and dim_time idempotently."""
    df_symbol = build_dim_symbol(spark=spark, config_path=config_path)
    if df_symbol.count() == 0:
        raise ValueError("[dim_symbol] Quality gate failed: empty dimension.")
    (
        df_symbol.coalesce(1)
        .write.mode("overwrite")
        .parquet(str(Path(gold_base_path) / "dim_symbol"))
    )

    df_time = build_dim_time(
        spark=spark,
        silver_base_path=silver_base_path,
        config_path=config_path,
    )
    if df_time.count() == 0:
        raise ValueError("[dim_time] Quality gate failed: empty dimension.")
    (
        df_time.coalesce(1)
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(str(Path(gold_base_path) / "dim_time"))
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("OmniLake-Gold-Dimensions")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    try:
        write_dimensions(spark)
    finally:
        spark.stop()
