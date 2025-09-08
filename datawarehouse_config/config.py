import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

DATAWAREHOUSE_PATH = os.getenv("DATAWAREHOUSE_PATH")
if os.path.exists(DATAWAREHOUSE_PATH):
    os.remove(DATAWAREHOUSE_PATH)

conn = duckdb.connect(database=DATAWAREHOUSE_PATH)

with open('/home/tiendat/Pipeline-Stock-ETL/etl_datawarehouse/gold/ddl_gold.sql', 'r') as file:
    sql_script = file.read()

for statement in sql_script.split(";"):
    stmt = statement.strip()
    if stmt:  # bỏ dòng rỗng
        conn.execute(stmt)

conn.close()