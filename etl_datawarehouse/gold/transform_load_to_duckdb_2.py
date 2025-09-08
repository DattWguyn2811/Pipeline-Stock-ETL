import logging
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

DATAWAREHOUSE_PATH = os.getenv("DATAWAREHOUSE_PATH")
USER = "tiendat"

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Transform and load data to Gold layer") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()


def read_silver_data(partition_date: str):
    """Read ohlcs data from Silver layer."""
    hdfs_path = f"/user/{USER}/datalake/elt/silver/ohlcs/dt={partition_date}"

    try:
        logging.info(f"Reading Silver data from {hdfs_path}")
        df = spark.read.parquet(hdfs_path)
        row_count = df.count()
        logging.info(f"Read {row_count} rows from Silver layer for partition {partition_date}")
        return df
    except Exception as e:
        logging.error(f"Failed to read data from Silver layer: {e}")
        return None


def process_data(df):
    """Processing ohlcs data for Gold layer."""
    if df is None:
        logging.warning("No data to transform. Skipping process.")
        return None    

    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    logging.info(f"Processing data for date: {yesterday}")

    conn = duckdb.connect(database=DATAWAREHOUSE_PATH)
    logging.info("Connected to DuckDB")

    # Insert dim_time if not exists
    logging.info("Ensuring dim_time has entry for yesterday")
    conn.execute(f"""
        insert into dim_time (time_date, time_day_of_week, time_month, time_quarter, time_year)
        select
            '{yesterday}',
            '{yesterday.strftime("%A")}',
            '{yesterday.strftime("%B")}',
            '{((yesterday.month - 1) // 3) + 1}',
            {yesterday.year}
        where not exists (
            select 1 
            from dim_time 
            where time_date = '{yesterday}'
        );
    """)

    # Get dim_time key
    time_key_df = conn.execute(f"""
        select time_key from dim_time where time_date = '{yesterday}'                                
    """).fetchdf()
    candle_time_key = time_key_df['time_key'][0]
    logging.info(f"Using time_key={candle_time_key} for partition {yesterday}")

    # Get companies mapping
    company_key_df = conn.execute("""
        select company_key, company_ticker from dim_companies
    """).fetchdf()
    logging.info(f"Fetched {len(company_key_df)} companies from dim_companies")

    companies_df = company_key_df.drop_duplicates(subset=['company_ticker'], keep='last')
    companies_df = spark.createDataFrame(companies_df)

    # Join with input data
    df = df.join(companies_df, on='company_ticker', how='left')
    before_filter = df.count()
    df = df.filter(df['company_key'].isNotNull())
    after_filter = df.count()
    logging.info(f"Joined with companies. Rows before filter={before_filter}, after filter={after_filter}")

    # Add candle_time_key
    df = df.withColumn('candle_time_key', F.lit(candle_time_key))
    logging.info("Added candle_time_key column")

    # Convert to Arrow Table
    arrow_table = pa.Table.from_pandas(df.toPandas())
    logging.info(f"Converted Spark DataFrame to Arrow Table with {arrow_table.num_rows} rows")

    conn.register("arrow_table", arrow_table)

    # Insert fact_candles
    logging.info("Inserting into fact_candles...")
    conn.execute("""
        insert into fact_candles (
            candle_company_key,
            candle_volume,
            candle_volume_weighted,
            candle_open,
            candle_close,
            candle_high,
            candle_low,
            candle_time_stamp,
            candle_num_of_trades,
            candle_is_otc,
            candle_time_key                      
        ) select
            company_key,
            candle_volume,
            candle_volume_weighted,
            candle_open,
            candle_close,
            candle_high,
            candle_low,
            candle_time_stamp,
            candle_num_of_trades,
            candle_is_otc,
            candle_time_key
        from arrow_table;             
    """)
    logging.info(f"Inserted {arrow_table.num_rows} rows into fact_candles")

    conn.close()
    logging.info("Closed DuckDB connection successfully")


def transform_load_to_duckdb():
    partition_date = datetime.date.today() - datetime.timedelta(days=1)

    df = read_silver_data(partition_date)
    if df:
        process_data(df)
    else:
        logging.warning("No data returned from Silver layer. Nothing loaded to DuckDB.")


if __name__ == "__main__":    
    transform_load_to_duckdb()