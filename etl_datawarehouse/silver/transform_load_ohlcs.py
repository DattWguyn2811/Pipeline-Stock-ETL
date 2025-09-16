import logging
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os

# Load .env
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

USER = "tiendat"

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Transform and load data to Silver layer") \
    .config("spark.sql.caseSensitive", "true") \
    .getOrCreate()  


def read_bronze_data(partition_date: str):
    """Read data from Bronze layer."""
    hdfs_base_path = f"/user/{USER}/datalake/elt/bronze/ohlcs"
    hdfs_path = f"{hdfs_base_path}/dt={partition_date}"
    try:
        df = spark.read.parquet(hdfs_path)
        logging.info(f"Read {df.count()} rows from Bronze layer")
        return df
    except Exception as e:
        logging.error(f"Failed to read data from Bronze layer: {e}")
        return None

def transform_data(df):
    """Transform data for Silver layer."""
    if df is None:
        logging.warning("No data to transform")
        return None
    
    df = (
        df.withColumn("otc", F.when(F.col("otc").isNull(), F.lit(False)).otherwise(F.col("otc"))) \
            .withColumn("n", F.when(F.col("n").isNull(), F.lit(0)).otherwise(F.col("n").cast("long"))) \
            .withColumn("v", F.when(F.col("v").isNull(), F.lit(0)).otherwise(F.col("v").cast("long"))) \
            .withColumn("vw", F.when(F.col("vw").isNull(), F.lit(0.0)).otherwise(F.col("vw"))) \
            .withColumn("t", F.from_unixtime(F.col("t") / 1000, "yyyy-MM-dd HH:mm:ss").cast("timestamp")) \
            .withColumn("T", F.trim(F.col("T")))
    )

    df = df.withColumnRenamed("T", "company_ticker") \
        .withColumnRenamed("v", "candle_volume") \
        .withColumnRenamed("vw", "candle_volume_weighted") \
        .withColumnRenamed("o", "candle_open") \
        .withColumnRenamed("c", "candle_close") \
        .withColumnRenamed("h", "candle_high") \
        .withColumnRenamed("l", "candle_low") \
        .withColumnRenamed("t", "candle_time_stamp") \
        .withColumnRenamed("n", "candle_num_of_trades") \
        .withColumnRenamed("otc", "candle_is_otc")

    df = df.dropDuplicates()

    logging.info(f"Transformed data: {df.count()} rows")
    return df


def save_to_silver(df, partition_date: str):
    """Save transformed data to Silver layer."""
    hdfs_path = f"/user/{USER}/datalake/elt/silver/ohlcs/dt={partition_date}"
    try:
        df.write.mode("overwrite").parquet(hdfs_path)
        logging.info(f"Saved {df.count()} rows to Silver layer at {hdfs_path}")
    except Exception as e:
        logging.error(f"Failed to save to Silver layer: {e}")


def transform_bronze_to_silver():
    partition_date = datetime.date.today() - datetime.timedelta(days=1)
    df = read_bronze_data(partition_date)
    if df:
        transformed_df = transform_data(df)
        if transformed_df:
            save_to_silver(transformed_df, partition_date)


if __name__ == "__main__":
    transform_bronze_to_silver()