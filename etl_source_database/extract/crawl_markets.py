import requests
import json
import datetime
from hdfs import InsecureClient
from dotenv import load_dotenv
import os 

def crawl_markets():

    API_TOKEN = os.getenv("MARKETSTATUS_API_KEY")

    # Fetch markets data
    url = f'https://www.alphavantage.co/query?function=MARKET_STATUS&apikey={API_TOKEN}'
    response = requests.get(url)
    data = response.json()["markets"]
    print(f"Extracted {len(data)} regions and exchanges.")

    # Get the current date for filename
    date = datetime.date.today().strftime("%Y-%m-%d")
    hdfs_path = f"/user/tiendat/etl_database/raw/marketstatus/crawl_marketstatus_{date}.json"

    # Serialize the market data to JSON
    json_object = json.dumps(data, indent=4)

    # Create HDFS client 
    client = InsecureClient('http://34.59.119.128:9870', user='tiendat')

    # Write file to HDFS
    with client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
        writer.write(json_object)

    print(f"Data saved to HDFS at {hdfs_path}")

crawl_markets()