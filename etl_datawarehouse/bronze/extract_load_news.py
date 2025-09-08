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
API_KEY = os.getenv("NEWS_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"

def requests_retry_session(
    retries=3,
    backoff_factor=0.5,
    status_forcelist=(500, 502, 504),
    session=None,
):
    """Retry logic for requests session."""
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

def get_data_by_time_range(base_date: datetime.date, time_zone: int) -> tuple[str, str]:
    """Return time_from and time_to strings for a given timezone."""
    if time_zone == 1:
        return base_date.strftime("%Y%m%dT0000"), base_date.strftime("%Y%m%dT2359")
    elif time_zone == 2:
        return base_date.strftime("%Y%m%dT0000"), base_date.strftime("%Y%m%dT1200")
    else:  # time_zone == 3
        return base_date.strftime("%Y%m%dT1201"), base_date.strftime("%Y%m%dT2359")

def fetch_news(base_date: datetime.date) -> pd.DataFrame | None:
    """Fetch news data for a given date (splitted into 3 time zones)."""
    json_object = []
    total = 0

    for time_zone in [1, 2, 3]:
        time_from, time_to = get_data_by_time_range(base_date, time_zone)
        logging.info(f"Fetching news from {time_from} to {time_to}")

        url = f"{BASE_URL}?function=NEWS_SENTIMENT&time_from={time_from}&time_to={time_to}&limit=1000&apikey={API_KEY}"

        try:
            response = requests_retry_session().get(url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logging.error(f"API request failed: {e}")
            return None

        data = response.json()
        feed = data.get("feed", [])

        if not feed:
            logging.warning(f"No news available for {base_date}")
            continue

        total += len(feed)

        if total == 1000 and time_zone == 1:
            continue  # still possible to fetch in later zones

        json_object.extend(feed)

        if total < 1000 and time_zone == 1:
            break  # no need to fetch further

    if not json_object:
        logging.warning("No news crawled at all")
        return None

    return pd.DataFrame(json_object)

def save_to_hdfs(df: pd.DataFrame, base_date: datetime.date):
    """Save DataFrame to HDFS in Parquet format, partitioned by date."""
    date_str = base_date.strftime("%Y-%m-%d")
    hdfs_dir = f"/user/{USER}/datalake/elt/bronze/news/dt={date_str}"
    hdfs_path = f"{hdfs_dir}/part-0000.parquet"

    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)

    try:
        client.makedirs(hdfs_dir)  # ensure partition folder exists
        with client.write(hdfs_path, overwrite=True) as writer:
            writer.write(buffer.read())
        logging.info(f"Saved {len(df)} news records to {hdfs_path}")
    except Exception as e:
        logging.error(f"Failed to save to HDFS: {e}")

def crawl_news():
    base_date = datetime.date.today() - datetime.timedelta(days=1)
    df = fetch_news(base_date)
    if df is not None and not df.empty:
        save_to_hdfs(df, base_date)

if __name__ == "__main__":
    crawl_news()