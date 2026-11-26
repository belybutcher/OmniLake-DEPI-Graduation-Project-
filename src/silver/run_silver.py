from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import BooleanType, DecimalType, LongType, StructField, StructType

from src.utils.config_loader import load_config
from src.utils.logger import get_logger

RAW_TRADE_SCHEMA = StructType([
    StructField("trade_id", LongType(), True),
    StructField("price", DecimalType(18, 8), True),
    StructField("quantity", DecimalType(18, 8), True),
    StructField("quote_quantity", DecimalType(18, 8), True),
    StructField("timestamp_ms", LongType(), True),
    StructField("is_buyer_maker", BooleanType(), True),
    StructField("is_best_match", BooleanType(), True),
])


def _resolve_path(raw_path: str, project_root: Path) -> Path:
    p = Path(raw_path)
    return p if p.is_absolute() else (project_root / p)


def _build_spark(cfg: dict) -> SparkSession:
    spark_cfg = cfg.get("spark", {})
    app_name = spark_cfg.get("app_name", "OmniLake-Market-Microstructure")
    master = spark_cfg.get("master", "local[*]")
    shuffle_partitions = int(spark_cfg.get("shuffle_partitions", 8))
    driver_memory = str(spark_cfg.get("driver_memory", "4g"))

    return (
        SparkSession.builder
        .appName(f"{app_name}-Silver")
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run_silver(
    config_path: str = "config/config.yamL",
    symbols: Sequence[str] | None = None,
    date_filter: str | None = None,
    overwrite: bool = False,
) -> None:
    logger = get_logger("SilverRunner")
    cfg = load_config(config_path)

    project_cfg = cfg.get("project", {})
    env = project_cfg.get("environment", "local")
    project_root = Path(project_cfg.get("root_path", Path.cwd()))

    storage_cfg = cfg.get("storage", {}).get(env, {})
    bronze_root = _resolve_path(storage_cfg.get("bronze", "data_lake/bronze"), project_root)
    silver_root = _resolve_path(storage_cfg.get("silver", "data_lake/silver"), project_root)

    silver_cfg = cfg.get("silver_processing", {})
    output_format = silver_cfg.get("output_format", "parquet")
    partition_cols = list(silver_cfg.get("partition_columns", ["symbol", "trade_date"]))
    quality_cfg = silver_cfg.get("quality_rules", {})
    drop_null_ids = bool(quality_cfg.get("drop_null_ids", True))
    min_price = float(quality_cfg.get("min_price", 0.000001))
    min_quantity = float(quality_cfg.get("min_quantity", 0.000001))

    target_symbols = (
        [s.upper().strip() for s in symbols]
        if symbols
        else [s.upper().strip() for s in cfg.get("ingestion", {}).get("symbols", ["BTCUSDT"])]
    )

    spark = _build_spark(cfg)
    try:
        processed_any = False
        for sym in target_symbols:
            sym_bronze_dir = bronze_root / sym
            if not sym_bronze_dir.exists():
                logger.warning(f"[{sym}] Bronze directory not found: {sym_bronze_dir} — skipping.")
                continue

            pattern = f"*{date_filter}*.csv" if date_filter else "*.csv"
            csv_files = sorted(str(p) for p in sym_bronze_dir.glob(pattern) if p.is_file())
            if not csv_files:
                logger.warning(f"[{sym}] No CSV files matching '{pattern}' in {sym_bronze_dir} — skipping.")
                continue

            logger.info(f"[{sym}] Reading {len(csv_files)} Bronze CSV file(s)...")
            df = (
                spark.read
                .option("header", "false")
                .schema(RAW_TRADE_SCHEMA)
                .csv(csv_files)
            )

            if drop_null_ids:
                df = df.filter(F.col("trade_id").isNotNull())

            df = df.filter(
                F.col("price").isNotNull()
                & F.col("quantity").isNotNull()
                & F.col("timestamp_ms").isNotNull()
                & (F.col("price") >= F.lit(min_price))
                & (F.col("quantity") >= F.lit(min_quantity))
            )

            df = (
                df.dropDuplicates(["trade_id"])
                .withColumn("symbol", F.lit(sym))
                .withColumn(
                    "event_timestamp",
                    F.to_timestamp(F.from_unixtime(F.col("timestamp_ms") / F.lit(1000.0))),
                )
                .withColumn("trade_date", F.date_format(F.col("event_timestamp"), "yyyy-MM-dd"))
            )

            if date_filter and len(date_filter.strip()) == 10:
                df = df.filter(F.col("trade_date") == F.lit(date_filter.strip()))

            mode = "overwrite" if overwrite else "append"
            logger.info(
                f"[{sym}] Writing Silver ({output_format}, mode={mode}) partitioned by {partition_cols} -> {silver_root}"
            )
            writer = df.write.format(output_format).mode(mode)
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(str(silver_root))
            processed_any = True
            logger.info(f"[{sym}] Silver layer write completed.")

        if not processed_any:
            raise RuntimeError("No Bronze data was processed into Silver for the requested filters.")

        logger.info("[SUCCESS] Silver pipeline finished.")
    finally:
        spark.stop()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OmniLake Silver Layer PySpark Runner")
    parser.add_argument("--config", type=str, default="config/config.yamL", help="Path to config YAML")
    parser.add_argument("--symbols", nargs="+", default=None, help="Symbols e.g. BTCUSDT")
    parser.add_argument("--date", type=str, default=None, help="Optional date/period filter e.g. 2024-01 or 2024-01-01")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing output")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        run_silver(
            config_path=args.config,
            symbols=args.symbols,
            date_filter=args.date,
            overwrite=args.overwrite,
        )
        sys.exit(0)
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)

