import logging
import datetime
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import duckdb
import pyarrow as pa

# Load .env
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DATAWAREHOUSE_PATH = os.getenv("DATAWAREHOUSE_PATH")
USER = "tiendat"

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Transform and load data to Gold layer") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()


def read_silver_data(partition_date: str):
    """Read data from Silver layer."""
    hdfs_path = f"/user/{USER}/datalake/elt/silver/database_source/dt={partition_date}"
    logger.info(f"Reading data from Silver layer: {hdfs_path}")

    try:
        df = spark.read.parquet(hdfs_path)
        row_count = df.count()
        logger.info(f"Successfully read {row_count} rows from Silver layer")
        return df
    except Exception as e:
        logger.error(f"Failed to read data from Silver layer path={hdfs_path}: {e}")
        return None


def process_data(df):
    """Processing companies data for Gold layer."""
    if df is None:
        logger.warning("No data to transform, skipping process_data()")
        return None    

    logger.info("Starting data transformation...")

    logger.debug("Printing schema and sample data:")
    df.printSchema()
    df.show(5, truncate=False)

    try:
        logger.info("Converting Spark DataFrame -> Pandas -> Arrow Table")
        arrow_table = pa.Table.from_pandas(df.toPandas())

        logger.info(f"Connecting to DuckDB at {DATAWAREHOUSE_PATH}")
        conn = duckdb.connect(database=DATAWAREHOUSE_PATH)

        conn.register("arrow_table", arrow_table)
        logger.info("Arrow table registered successfully in DuckDB")

        logger.info("Inserting data into dim_companies...")
        conn.execute("""
            insert into dim_companies (
                company_name,
                company_ticker,
                company_is_delisted,
                company_category,
                company_currency,
                company_location,
                company_exchange_name,
                company_region_name,
                company_industry_name,
                company_industry_sector,
                company_sic_industry,
                company_sic_sector
            ) select 
                company_name,
                company_ticker,
                company_is_delisted,
                company_category,
                company_currency,
                company_location,
                company_exchange_name,
                company_region_name,
                company_industry_name,
                company_industry_sector,
                company_sic_industry,
                company_sic_sector
            from arrow_table;
        """)

        conn.close()
        row_count = df.count()
        logger.info(f"Inserted {row_count} rows into dim_companies successfully")

    except Exception as e:
        logger.error(f"Error while processing and inserting data: {e}", exc_info=True)


def transform_load_to_duckdb():
    partition_date = datetime.date.today()

    df = read_silver_data(partition_date)
    if df:
        process_data(df)
    else:
        logger.warning("No DataFrame returned from read_silver_data, skipping process_data()")


if __name__ == "__main__":
    transform_load_to_duckdb()