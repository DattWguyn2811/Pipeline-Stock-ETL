import psycopg2
import json
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


def insert_processed_data_to_database(hdfs_directory, table_name, columns, conflict_columns):
    with client.read(hdfs_directory, encoding='utf-8') as reader:
        data = [
            json.loads(line)
            for line in reader
            if line.strip()
        ]
    
    if not data:
        print(f"No data found in {hdfs_directory}")
        return 

    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)
    conflict_columns_str = ', '.join(conflict_columns)

    sql_statement = f"""
        insert into {table_name} ({columns_str})
        values ({placeholders})
        on conflict ({conflict_columns_str}) do nothing 
    """

    conn = psycopg2.connect(
        host="localhost",
        database="datasource",
        user="postgres",
        password="postgres"
    )

    cur = conn.cursor()

    
    for record in data:
        values = [record[col] for col in columns]
        cur.execute(sql_statement, values)
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted data into {table_name}")


def load_json_to_database_3():
    columns = [
        "company_exchange_id", "company_industry_id", "company_sic_id", 
        "company_name", "company_ticker", "company_is_delisted", 
        "company_category", "company_currency", "company_location"
    ]

    # Insert data into `companies` table
    insert_processed_data_to_database(
        get_lastest_file_in_hdfs('/user/tiendat/etl_database/processed/transformed_to_database_companies', '.json'),
        'etl.companies',
        columns,
        ['company_ticker', 'company_is_delisted']
    )

load_json_to_database_3()