import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonVirtualenvOperator
import sys
import os

def run_extract_load_news():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_datawarehouse/bronze")
    from extract_load_news import crawl_news
    crawl_news()

def run_extract_load_ohlcs():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_datawarehouse/bronze")
    from extract_load_ohlcs import crawl_ohlcs
    crawl_ohlcs()

def run_extract_load_source_database():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_datawarehouse/bronze")
    from extract_load_source_database import extract_load_source_database
    extract_load_source_database()

def run_transform_load_news():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_datawarehouse/silver")
    from transform_load_news import transform_bronze_to_silver
    transform_bronze_to_silver()

def run_transform_load_ohlcs():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_datawarehouse/silver")
    from transform_load_ohlcs import transform_bronze_to_silver
    transform_bronze_to_silver()

def run_transform_load_source_database():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_datawarehouse/silver")
    from transform_load_source_database import transform_bronze_to_silver
    transform_bronze_to_silver()

def run_transform_load_to_duckdb_1():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_datawarehouse/gold")
    from transform_load_to_duckdb_1 import transform_load_to_duckdb
    transform_load_to_duckdb()

def run_transform_load_to_duckdb_2():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_datawarehouse/gold")
    from transform_load_to_duckdb_2 import transform_load_to_duckdb
    transform_load_to_duckdb()

def run_transform_load_to_duckdb_3():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_datawarehouse/gold")
    from transform_load_to_duckdb_3 import transform_load_to_duckdb
    transform_load_to_duckdb()

req_file = "/home/tiendat/requirements.txt"
with open(req_file) as f:
    requirements = f.read().splitlines()

env_vars = {
    "JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64",
    "SPARK_HOME": "/usr/local/spark",
    "HADOOP_HOME": "/usr/local/hadoop",
    "PYTHONPATH": "/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip",
    "PATH": "/bin:/usr/bin:/usr/local/spark/bin:/usr/local/spark/sbin:/usr/lib/jvm/java-11-openjdk-amd64/bin:" + os.environ.get("PATH", ""),
    "SPARK_LOCAL_DIRS": "/tmp",
    "SPARK_WORKER_DIR": "/tmp",
    "PYSPARK_PYTHON": "/usr/bin/python3",
    "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
    "SPARK_CONF_DIR": "/usr/local/spark/conf",
    "SHELL": "/bin/bash"  # Explicitly set shell
}

default_args = {
    'owner' : 'tiendat',
    'retries' : 1,
    'retry_delay' : datetime.timedelta(minutes=2),
}

with DAG(
    dag_id="etl_datawarehouse",
    description="A DAG for ETL pipeline for datawarehouse",
    default_args=default_args,
    schedule="1 1 * * *",
    start_date=datetime.datetime(2025,9,1),
    catchup=False
) as dag:
    # Bronze Layer
    extract_load_news_task = PythonVirtualenvOperator(
        task_id = "extract_load_news",
        python_callable = run_extract_load_news,
        requirements = requirements,
        system_site_packages = True,
        inherit_env = True 
    )

    extract_load_ohlcs_task = PythonVirtualenvOperator(
        task_id = "extract_load_ohlcs",
        python_callable = run_extract_load_ohlcs,
        requirements = requirements,
        system_site_packages = True,
        inherit_env = True
    )

    extract_load_source_database_task = PythonVirtualenvOperator(
        task_id = "extract_load_source_database",
        python_callable = run_extract_load_source_database,
        requirements = requirements,
        system_site_packages = True,
        inherit_env = True
    )

    # Silver Layer
    transform_load_news_task = PythonVirtualenvOperator(
        task_id = "transform_load_news",
        python_callable = run_transform_load_news,
        requirements = requirements,
        system_site_packages = True,
        env_vars = env_vars,
        inherit_env = True
    )

    transform_load_ohlcs_task = PythonVirtualenvOperator(
        task_id = "transform_load_ohlcs",
        python_callable = run_transform_load_ohlcs,
        requirements = requirements,
        system_site_packages = True,
        env_vars = env_vars,
        inherit_env = True
    )

    transform_load_source_database_task = PythonVirtualenvOperator(
        task_id = "transform_load_source_database",
        python_callable = run_transform_load_source_database,
        requirements = requirements,
        system_site_packages = True,
        env_vars = env_vars,
        inherit_env = True
    )
        
    # Gold Layer 
    transform_load_to_duckdb_1_task = PythonVirtualenvOperator(
        task_id = "transform_load_to_duckdb_1",
        python_callable = run_transform_load_to_duckdb_1,
        requirements = requirements,
        system_site_packages = True,
        env_vars = env_vars,
        inherit_env = True
    )

    transform_load_to_duckdb_2_task = PythonVirtualenvOperator(
        task_id = "transform_load_to_duckdb_2",
        python_callable = run_transform_load_to_duckdb_2,
        requirements = requirements,
        system_site_packages = True,
        env_vars = env_vars,
        inherit_env = True
    )
        

    transform_load_to_duckdb_3_task = PythonVirtualenvOperator(
        task_id = "transform_load_to_duckdb_3",
        python_callable = run_transform_load_to_duckdb_3,
        requirements = requirements,
        system_site_packages = True,
        env_vars = env_vars,
        inherit_env = True
    )

    (extract_load_news_task >> 
    extract_load_ohlcs_task >> 
    extract_load_source_database_task >> 
    transform_load_news_task >> 
    transform_load_ohlcs_task >> 
    transform_load_source_database_task >> 
    transform_load_to_duckdb_1_task >> 
    transform_load_to_duckdb_2_task >> 
    transform_load_to_duckdb_3_task)