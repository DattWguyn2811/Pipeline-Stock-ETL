import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonVirtualenvOperator
import sys
import os

def run_crawl_companies():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_source_database/extract")
    from crawl_companies import crawl_companies
    crawl_companies()

def run_crawl_markets():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_source_database/extract")
    from crawl_markets import crawl_markets
    crawl_markets()

def run_transform_data_1():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_source_database/transform")
    from transform_data_1 import transform_data_1
    transform_data_1()

def run_transform_data_2():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_source_database/transform")
    from transform_data_2 import transform_data_2
    transform_data_2()

def run_transform_data_3():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_source_database/transform")
    from transform_data_3 import transform_data_3
    transform_data_3()

def run_load_json_to_database_1():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_source_database/load")
    from load_json_to_database_1 import load_json_to_database_1
    load_json_to_database_1()

def run_load_json_to_database_2():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_source_database/load")
    from load_json_to_database_2 import load_json_to_database_2
    load_json_to_database_2()

def run_load_json_to_database_3():
    sys.path.append("/home/tiendat/Pipeline-Stock-ETL/etl_source_database/load")
    from load_json_to_database_3 import load_json_to_database_3
    load_json_to_database_3()

req_file = "/home/tiendat/requirements.txt"
with open(req_file) as f:
    requirements = f.read().splitlines()

default_args = {
    'owner' : 'tiendat',
    'retries' : 1,
    'retry_delay' : datetime.timedelta(seconds=30),
}

with DAG(
    dag_id="elt_source_database",
    description="A DAG for ETL pipeline for source datanbase",
    default_args=default_args,
    schedule="0 1 * * *",
    start_date=datetime.datetime(2025,9,1),
    catchup=False
) as dag:
    crawl_companies_task = PythonVirtualenvOperator(
        task_id = "crawl_companies_task",
        python_callable = run_crawl_companies,
        requirements = requirements,
        system_site_packages = False
    )

    crawl_markets_task = PythonVirtualenvOperator(
        task_id = "crawl_markets_task",
        python_callable = run_crawl_markets,
        requirements = requirements,
        system_site_packages = False
    )

    transform_data_1_task = PythonVirtualenvOperator(
        task_id = "transform_data_1",
        python_callable = run_transform_data_1,
        requirements = requirements,
        system_site_packages = False
    )

    load_json_to_database_1_task = PythonVirtualenvOperator(
        task_id = "load_json_to_database_1",
        python_callable = run_load_json_to_database_1,
        requirements = requirements,
        system_site_packages = False
    )

    transform_data_2_task = PythonVirtualenvOperator(
        task_id = "transform_data_2",
        python_callable = run_transform_data_2,
        requirements = requirements,
        system_site_packages = False
    )

    load_json_to_database_2_task = PythonVirtualenvOperator(
        task_id = "load_json_to_database_2",
        python_callable = run_load_json_to_database_2,
        requirements = requirements,
        system_site_packages = False
    )

    transform_data_3_task = PythonVirtualenvOperator(
        task_id = "transform_data_3",
        python_callable = run_transform_data_3,
        requirements = requirements,
        system_site_packages = False
    )

    load_json_to_database_3_task = PythonVirtualenvOperator(
        task_id = "load_json_to_database_3",
        python_callable = run_load_json_to_database_3,
        requirements = requirements,
        system_site_packages = False
    )

    crawl_companies_task >> crawl_markets_task >> transform_data_1_task >> load_json_to_database_1_task >> transform_data_2_task >> load_json_to_database_2_task >> transform_data_3_task >> load_json_to_database_3_task

