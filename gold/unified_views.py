from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import yaml


def load_symbols_from_config(config_path: str = "config/symbols.yaml") -> list[str]:
    """Load configured symbols without hardcoding symbol lists in pipeline code."""
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    items = data.get("symbols", [])
    return [item["symbol"] for item in items if isinstance(item, dict) and "symbol" in item]


def build_unified_ohlcv(
    spark: SparkSession,
    interval: str,
    target_date: str,
    symbols: list[str] | None = None,
    gold_base_path: str = "data_lake/gold",
    config_path: str = "config/symbols.yaml",
) -> DataFrame:
    """Merge available symbol OHLCV partitions for a given interval and date."""
    candidate_symbols = symbols or load_symbols_from_config(config_path)
    dfs: list[DataFrame] = []

    for sym in candidate_symbols:
        sym_path = Path(gold_base_path) / f"fct_ohlcv_{interval}" / sym
        if not sym_path.exists():
            continue
        df_sym = (
            spark.read.parquet(str(sym_path))
            .filter(F.col("date") == F.lit(target_date))
            .withColumn("symbol", F.lit(sym))
        )
        if len(df_sym.take(1)) > 0:
            dfs.append(df_sym)

    if not dfs:
        raise ValueError(
            f"[fct_ohlcv_{interval}_unified] No data found for date={target_date} across symbols={candidate_symbols}."
        )

    unified_df = dfs[0]
    for extra_df in dfs[1:]:
        unified_df = unified_df.unionByName(extra_df, allowMissingColumns=True)

    return unified_df


def write_unified_ohlcv(
    df: DataFrame,
    interval: str,
    gold_base_path: str = "data_lake/gold",
    num_partitions: int = 1,
) -> None:
    """Write unified OHLCV table idempotently partitioned by date."""
    out_path = str(Path(gold_base_path) / f"fct_ohlcv_{interval}_unified")
    writer = df.coalesce(num_partitions) if num_partitions == 1 else df.repartition(num_partitions)
    (
        writer.write.mode("overwrite")
        .partitionBy("date")
        .parquet(out_path)
    )
