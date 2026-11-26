import argparse
from pyspark.sql import SparkSession

from gold.daily_summary import build_daily_summary, write_daily_summary
from gold.dimensions import write_dimensions
from gold.ohlcv import build_ohlcv, write_ohlcv
from gold.unified_views import build_unified_ohlcv, write_unified_ohlcv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run OmniLake Gold layer aggregations.")
    parser.add_argument("--symbol", required=True, help="Trading symbol (e.g., BTCUSDT)")
    parser.add_argument(
        "--interval",
        default="1h",
        choices=["1m", "5m", "15m", "1h", "4h", "1d"],
        help="OHLCV aggregation interval",
    )
    parser.add_argument("--date", required=True, help="Target trade date (YYYY-MM-DD)")
    parser.add_argument("--silver-path", default="data_lake/silver", help="Silver base path")
    parser.add_argument("--gold-path", default="data_lake/gold", help="Gold base path")
    parser.add_argument("--config-path", default="config/symbols.yaml", help="Symbols YAML path")
    parser.add_argument("--num-partitions", type=int, default=1, help="Output Parquet partitions")
    parser.add_argument("--with-daily", action="store_true", help="Build and write daily summary")
    parser.add_argument("--with-unified", action="store_true", help="Build and write unified OHLCV")
    parser.add_argument("--with-dimensions", action="store_true", help="Build and write dimensions")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName(f"OmniLake-Gold-{args.symbol}-{args.interval}-{args.date}")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    try:
        ohlcv_df = build_ohlcv(
            spark=spark,
            symbol=args.symbol,
            interval=args.interval,
            target_date=args.date,
            silver_base_path=args.silver_path,
        )
        write_ohlcv(
            df=ohlcv_df,
            symbol=args.symbol,
            interval=args.interval,
            gold_base_path=args.gold_path,
            num_partitions=args.num_partitions,
        )

        if args.with_daily:
            daily_df = build_daily_summary(
                spark=spark,
                symbol=args.symbol,
                target_date=args.date,
                silver_base_path=args.silver_path,
            )
            write_daily_summary(
                df=daily_df,
                symbol=args.symbol,
                gold_base_path=args.gold_path,
            )

        if args.with_unified:
            unified_df = build_unified_ohlcv(
                spark=spark,
                interval=args.interval,
                target_date=args.date,
                symbols=None,
                gold_base_path=args.gold_path,
                config_path=args.config_path,
            )
            write_unified_ohlcv(
                df=unified_df,
                interval=args.interval,
                gold_base_path=args.gold_path,
                num_partitions=args.num_partitions,
            )

        if args.with_dimensions:
            write_dimensions(
                spark=spark,
                gold_base_path=args.gold_path,
                silver_base_path=args.silver_path,
                config_path=args.config_path,
            )

        print(f"[SUCCESS] Gold finished for {args.symbol} ({args.interval}) on {args.date}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
