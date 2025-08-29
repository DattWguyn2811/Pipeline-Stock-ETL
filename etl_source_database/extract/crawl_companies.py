import requests
import json
import datetime
from hdfs import InsecureClient
from dotenv import load_dotenv
import os 

def crawl_companies():

    API_TOKEN = os.getenv("SEC_API_KEY")

    # List of stock exchanges to extract data from
    exchanges = ["nasdaq", "nyse"]

    # Initialize an empty list to hold company data
    list_companies = []

    # Iterate over each exchange and fetch company data
    for exchange in exchanges:
        url = f'https://api.sec-api.io/mapping/exchange/{exchange}?token={API_TOKEN}'
        response = requests.get(url)
        data = response.json()
        list_companies.extend(data)
        print(f"Extracted {len(data)} companies from the {exchange.upper()} stock exchange.")

    # Get the current date for filename
    date = datetime.date.today().strftime("%Y-%m-%d")
    hdfs_path = f"/user/tiendat/etl_database/raw/companies/crawl_companies_{date}.json"

    # Serialize the list of companies to JSON
    json_object = json.dumps(list_companies, indent=4)

    # Create HDFS client 
    client = InsecureClient('http://34.59.119.128:9870', user='tiendat')

    # Write file to HDFS
    with client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
        writer.write(json_object)

    print(f"Data saved to HDFS at {hdfs_path}")

crawl_companies()