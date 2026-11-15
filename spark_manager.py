
from pyspark.sql import SparkSession
from src.utils.config_loader import load_config

def get_spark_session(app_name: str = None) -> SparkSession:
    """
    Creates or retrieves a standardized SparkSession for OmniLake components.
    """
    config = load_config()
    spark_config = config.get("spark", {})
    
    name = app_name or spark_config.get("app_name", "OmniLake-Quant-Pipeline")
    master = spark_config.get("master", "local[*]")
    shuffle_partitions = spark_config.get("shuffle_partitions", 8)
    
    spark = SparkSession.builder \
        .appName(name) \
        .master(master) \
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions)) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
        
    # تقليل رسائل التنبيه الزائدة في الكونسول لتنظيف مخرجات التنفيذ
    spark.sparkContext.setLogLevel("WARN")
    
    return spark
