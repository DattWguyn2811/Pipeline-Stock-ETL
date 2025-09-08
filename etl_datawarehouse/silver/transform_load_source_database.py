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
    .getOrCreate()  


def read_bronze_data(partition_date: str):
    """Read data from Bronze layer."""
    hdfs_path = f"/user/{USER}/datalake/elt/bronze/database_source/dt={partition_date}"
    try:
        df = spark.read.parquet(hdfs_path)
        logging.info(f"Read {df.count()} rows of source database from Bronze layer")
        return df
    except Exception as e:
        logging.error(f"Failed to read source database from Bronze layer: {e}")
        return None

def transform_data(df):
    """Transform data for Silver layer."""
    if df is None:
        logging.warning("No data to transform")
        return None
    
    trim_cols = [
        "company_name", "company_ticker", "company_category",
        "company_exchange_name", "company_industry_name",
        "company_industry_sector", "company_sic_industry",
        "company_sic_sector"
    ]
    for c in trim_cols:
        df = df.withColumn(c, F.trim(F.col(c)))
    
    df = df.withColumn(
        "company_location",
        F.regexp_replace(F.col("company_location"), "U\\.S\\.A", "United States")
    )

    df = df.dropDuplicates()

    logging.info(f"Transformed data: {df.count()} rows")
    return df


def save_to_silver(df, partition_date: str):
    """Save transformed data to Silver layer."""
    hdfs_path = f"/user/{USER}/datalake/elt/silver/database_source/dt={partition_date}"
    try:
        df.write.mode("overwrite").parquet(hdfs_path)
        logging.info(f"Saved {df.count()} rows to Silver layer at {hdfs_path}")
    except Exception as e:
        logging.error(f"Failed to save to Silver layer: {e}")


def transform_bronze_to_silver():
    partition_date = datetime.date.today()
    df = read_bronze_data(partition_date)
    if df:
        transformed_df = transform_data(df)
        if transformed_df:
            save_to_silver(transformed_df, partition_date)


if __name__ == "__main__":
    transform_bronze_to_silver()