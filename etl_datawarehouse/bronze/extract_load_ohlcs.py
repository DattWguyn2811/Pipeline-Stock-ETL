import requests
import datetime
import logging
from hdfs import InsecureClient
import os
from dotenv import load_dotenv
import pandas as pd
from io import BytesIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load .env
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# HDFS client
HDFS_URL = "http://34.59.119.128:9870"
USER = "tiendat"
client = InsecureClient(HDFS_URL, user=USER)

# API config
API_KEY = os.getenv("POLYGON_API_KEY")
BASE_URL = "https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks"

# Retry session for requests
def requests_retry_session(
    retries=3,
    backoff_factor=0.5,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def fetch_ohlcs(date: str) -> pd.DataFrame | None:
    """Fetch OHLC data from Polygon API for a given date."""
    url = f"{BASE_URL}/{date}?adjusted=true&include_otc=true&apiKey={API_KEY}"

    try:
        response = requests_retry_session().get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"API request failed: {e}")
        return None

    data = response.json()
    results = data.get("results", [])
    if not results:
        logging.warning(f"No data available for {date}")
        return None

    return pd.DataFrame(results)

def save_to_hdfs(df: pd.DataFrame, date: str):
    """Save DataFrame to HDFS as parquet, partitioned by date."""
    hdfs_dir = f"/user/{USER}/datalake/elt/bronze/ohlcs/dt={date}"
    hdfs_path = f"{hdfs_dir}/part-0000.parquet"

    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    try:
        client.makedirs(hdfs_dir)  # ensure partition folder exists
        with client.write(hdfs_path, overwrite=True) as writer:
            writer.write(buffer.read())
        logging.info(f"Saved {len(df)} OHLC records to {hdfs_path}")
    except Exception as e:
        logging.error(f"Failed to save to HDFS: {e}")

def crawl_ohlcs():
    # Yesterday (US market close ~4 AM VN)
    date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    df = fetch_ohlcs(date)
    if df is not None and not df.empty:
        save_to_hdfs(df, date)

if __name__ == "__main__":
    crawl_ohlcs()