import json
import pandas as pd
import numpy as np
import datetime
from hdfs import InsecureClient
from sqlalchemy import create_engine


hdfs_url = "http://34.59.119.128:9870"
user = "tiendat"
client = InsecureClient(hdfs_url, user=user)


def get_lastest_file_in_hdfs(hdfs_directory, extension):
    files = client.list(hdfs_directory, status=True)
    
    filtered_files = [
        (meta["modificationTime"], name)
        for name, meta in files
        if name.endswith(extension)
    ]

    if not filtered_files:
        return None

    latest_file = max(filtered_files, key=lambda x: x[0])
    
    return f"{hdfs_directory}/{latest_file[1]}"


def read_latest_file_in_hdfs(hdfs_directory):
    extension = '.json'
    latest_file = get_lastest_file_in_hdfs(hdfs_directory, extension)

    if not latest_file:
        print("No file found")
        return []
    
    with client.read(latest_file, encoding='utf-8') as reader:
        content = reader.read()
        data_json = json.loads(content)
    
    return data_json


def clean_dataframe(df):
    return df.replace(r'^\s*$', np.nan, regex=True).drop_duplicates().dropna()


def save_to_json(df, hdfs_directory):
    json_data = df.to_json(orient="records", lines=True)

    with client.write(hdfs_directory, encoding="utf-8", overwrite=True) as writer:
        writer.write(json_data)

    print(f"Saved dataframe to {hdfs_directory}")


def transform_data_3():
    companies_data = read_latest_file_in_hdfs("/user/tiendat/etl_database/raw/companies")

    date = datetime.date.today().strftime("%Y-%m-%d")

    companies = clean_dataframe(
        pd.DataFrame(
            [
                {
                    "company_exchange": item["exchange"], 
                    "company_industry": item["industry"], 
                    "company_sector": item["sector"], 
                    "company_sic_id": item["sic"], 
                    "company_name": item["name"], 
                    "company_ticker": item["ticker"], 
                    "company_is_delisted": item["isDelisted"], 
                    "company_category": item["category"], 
                    "company_currency": item["currency"], 
                    "company_location": item["location"]
                }
                for item in companies_data
            ]
        )
    )

    companies = companies[companies["company_exchange"].isin(["NYSE", "NASDAQ"])]
    companies = companies[companies["company_currency"] == "USD"]

    user = 'postgres'
    password = 'postgres'
    host = 'localhost'
    port = '5432'
    database = 'datasource'

    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn_str)

    query_exchanges = "select * from etl.exchanges"
    query_industries = "select * from etl.industries"

    exchanges = pd.read_sql(query_exchanges, engine)
    industries = pd.read_sql(query_industries, engine)

    companies = pd.merge(
        companies,
        exchanges,
        left_on="company_exchange",
        right_on="exchange_name"
    )[
        [
            "exchange_id",
            "company_industry", 
            "company_sector", 
            "company_sic_id", 
            "company_name", 
            "company_ticker", 
            "company_is_delisted", 
            "company_category", 
            "company_currency", 
            "company_location"
        ]
    ]

    companies = pd.merge(
        companies, 
        industries, 
        left_on=["company_industry", "company_sector"], 
        right_on=["industry_name", "industry_sector"],
        how='left'
    )[
        [
            "exchange_id", 
            "industry_id", 
            "company_sic_id", 
            "company_name", 
            "company_ticker", 
            "company_is_delisted", 
            "company_category", 
            "company_currency", 
            "company_location"
        ]
    ]

    companies = clean_dataframe(companies)

    new_columns = {
        "exchange_id" : "company_exchange_id",
        "industry_id" : "company_industry_id"
    }
    companies.rename(columns=new_columns, inplace=True)

    hdfs_path = f"/user/tiendat/etl_database/processed/transformed_to_database_companies/processed_companies_{date}.json"
    save_to_json(companies, hdfs_path)


transform_data_3()



