import psycopg2
import pandas as pd
import logging
from hdfs import InsecureClient
from datetime import date
from io import BytesIO
import os
from dotenv import load_dotenv

# Load .env
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# HDFS client
HDFS_URL = os.getenv("HDFS_URL", "http://34.59.119.128:9870")
USER = os.getenv("HDFS_USER", "tiendat")
client = InsecureClient(HDFS_URL, user=USER)

# Database config
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "host": os.getenv("DB_HOST"),
    "port": "5432",
}

# SQL query
QUERY = """
select 
    c.company_name,
    c.company_ticker,
    c.company_is_delisted,
    c.company_category,
    c.company_currency,
    c.company_location,
    e.exchange_name as company_exchange_name,
    r.region_name as company_region_name,
    i.industry_name as company_industry_name,
    i.industry_sector as company_industry_sector,
    s.sic_industry as company_sic_industry,
    s.sic_sector as company_sic_sector
from etl.companies c
left join etl.exchanges e 
on e.exchange_id = c.company_exchange_id
left join etl.regions r 
on r.region_id = e.exchange_region_id
left join etl.industries i
on i.industry_id = c.company_industry_id
left join etl.sic_industries s 
on s.sic_id = c.company_sic_id
-- where c.database_update_timestamp >= current_date
--  and c.database_update_timestamp < current_date + interval '1 day';
"""

def fetch_data() -> pd.DataFrame | None:
    """Extract data from PostgreSQL into a DataFrame."""
    try:
        logging.info("Connecting to PostgreSQL...")
        with psycopg2.connect(**DB_CONFIG) as conn:
            df = pd.read_sql(QUERY, conn)
        logging.info(f"Fetched {len(df)} rows from database")
        return df
    except Exception as e:
        logging.error(f"Database extraction failed: {e}")
        return None

def save_to_hdfs(df: pd.DataFrame, partition_date: str):
    """Save DataFrame to HDFS as Parquet, partitioned by date."""
    hdfs_dir = f"/user/{USER}/datalake/elt/bronze/database_source/dt={partition_date}"
    hdfs_path = f"{hdfs_dir}/part-0000.parquet"

    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    try:
        client.makedirs(hdfs_dir)
        with client.write(hdfs_path, overwrite=True) as writer:
            writer.write(buffer.read())
        logging.info(f"Saved {len(df)} records to {hdfs_path}")
    except Exception as e:
        logging.error(f"Failed to save to HDFS: {e}")

def extract_load_source_database():
    partition_date = date.today().strftime("%Y-%m-%d")
    df = fetch_data()
    if df is not None and not df.empty:
        save_to_hdfs(df, partition_date)

if __name__ == "__main__":
    extract_load_source_database()