import json
import pandas as pd
import numpy as np
import datetime
from hdfs import InsecureClient


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


def transform_data_1():
    companies_data = read_latest_file_in_hdfs("/user/tiendat/etl_database/raw/companies")
    markets_data = read_latest_file_in_hdfs("/user/tiendat/etl_database/raw/marketstatus")

    date = datetime.date.today().strftime("%Y-%m-%d")

    # Transform and save `regions` data
    regions = clean_dataframe(
        pd.DataFrame(
            [
                {
                    "region_name" : item["region"],
                    "region_local_open" : item["local_open"],
                    "region_local_close" : item["local_close"]
                }
                for item in markets_data
            ]
        )
    )
    regions_path = f"/user/tiendat/etl_database/processed/transformed_to_database_regions/processed_regions_{date}.json"
    save_to_json(regions, regions_path)

    # Transform and save `industries` data
    industries = clean_dataframe(
        pd.DataFrame(
            [
                {
                    "industry_name" : item["industry"],
                    "industry_sector" : item["sector"]
                }
                for item in companies_data    
            ]
        )
    )
    industries_path = f"/user/tiendat/etl_database/processed/transformed_to_database_industries/processed_industries_{date}.json"
    save_to_json(industries, industries_path)

    # Transform and save `sic_industries` data
    sic_industries = clean_dataframe(
        pd.DataFrame(
            [
                {
                    "sic_id": item["sic"],
                    "sic_industry" : item["sicIndustry"],
                    "sic_sector" : item["sicSector"]
                }
                for item in companies_data
            ]
        )
    )
    sic_industries_path = f"/user/tiendat/etl_database/processed/transformed_to_database_sic_industries/processed_sic_industries_{date}.json"
    save_to_json(sic_industries, sic_industries_path)

transform_data_1()
