cat << 'EOF' > gold/run_gold.py
import argparse
from pyspark.sql import SparkSession
from gold.daily_summary import build_daily_summary, validate_daily_summary, write_daily_summary
from gold.ohlcv import build_ohlcv, write_ohlcv
from gold.quality import validate_gold
from gold.unified_views import build_unified_ohlcv, write_unified_ohlcv


def main() -> None:
    parser = argparse.ArgumentParser(description="OmniLake Gold Layer Runner")
    parser.add_argument("--symbol", required=True, help="Trading pair symbol, e.g., BTCUSDT")
    parser.add_argument(
        "--interval",
        required=True,
        choices=["1m", "5m", "15m", "1h", "4h", "1d"],
        help="Candle interval",
    )
    parser.add_argument("--date", required=True, help="Target date in YYYY-MM-DD format")
    parser.add_argument("--silver-path", default="data_lake/silver", help="Base Silver directory")
    parser.add_argument("--gold-path", default="data_lake/gold", help="Base Gold directory")
    parser.add_argument("--config-path", default="config/symbols.yaml", help="Symbols YAML config path")
    parser.add_argument("--with-daily", action="store_true", help="Also build and write fct_trades_daily")
    parser.add_argument("--with-unified", action="store_true", help="Also build and write fct_ohlcv_{interval}_unified")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName(f"OmniLake-Gold-{args.symbol}-{args.interval}")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        df_ohlcv = build_ohlcv(
            spark=spark,
            symbol=args.symbol,
            interval=args.interval,
            target_date=args.date,
            silver_base_path=args.silver_path,
        )
        validate_gold(df_ohlcv, f"fct_ohlcv_{args.interval}_{args.symbol}")
        write_ohlcv(
            df=df_ohlcv,
            symbol=args.symbol,
            interval=args.interval,
            gold_base_path=args.gold_path,
        )

        if args.with_daily:
            df_daily = build_daily_summary(
                spark=spark,
                symbol=args.symbol,
                target_date=args.date,
                silver_base_path=args.silver_path,
            )
            validate_daily_summary(df_daily, f"fct_trades_daily_{args.symbol}")
            write_daily_summary(
                df=df_daily,
                symbol=args.symbol,
                gold_base_path=args.gold_path,
            )

        if args.with_unified:
            df_unified = build_unified_ohlcv(
                spark=spark,
                interval=args.interval,
                target_date=args.date,
                gold_base_path=args.gold_path,
                config_path=args.config_path,
            )
            validate_gold(df_unified, f"fct_ohlcv_{args.interval}_unified")
            write_unified_ohlcv(
                df=df_unified,
                interval=args.interval,
                gold_base_path=args.gold_path,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
EOF