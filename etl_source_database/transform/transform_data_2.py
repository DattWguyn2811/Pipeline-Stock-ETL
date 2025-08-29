import json
import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine
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


def transform_to_database_2():
    markets_data = read_latest_file_in_hdfs("/user/tiendat/etl_database/raw/marketstatus")

    date = datetime.date.today().strftime("%Y-%m-%d")

    exchanges = clean_dataframe(
        pd.DataFrame(
            [
                {
                    "region" : item["region"],
                    "primary_exchanges" : item["primary_exchanges"]
                }
                for item in markets_data
            ]
        )
    )

    exchanges = exchanges.assign(
        primary_exchanges = exchanges['primary_exchanges'].str.split(', ')
    )
    exchanges = exchanges.explode('primary_exchanges').reset_index(drop=True)

    user = 'postgres'
    password = 'postgres'
    host = 'localhost'
    port = '5432'
    database = 'datasource'

    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn_str)

    query = "select * from etl.regions"
    regions = pd.read_sql(query, engine)

    exchanges = pd.merge(
        exchanges,
        regions,
        left_on="region",
        right_on="region_name"
    )[
        ["region_id", "primary_exchanges"]
    ]
    
    new_columns = {'region_id': 'exchange_region_id', 'primary_exchanges': 'exchange_name'}
    exchanges.rename(columns=new_columns, inplace=True)

    hdfs_path = f"/user/tiendat/etl_database/processed/transformed_to_database_exchanges/processed_exchanges_{date}.json"
    save_to_json(exchanges, hdfs_path)

transform_to_database_2()