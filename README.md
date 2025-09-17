# Pipeline-Stock-ETL

## Overview

**Pipeline-Stock-ETL** is an automated ETL (Extract - Transform - Load) system for collecting, processing, and storing financial, stock, and news data from various sources into a Data Warehouse (DuckDB). The system supports multi-layer data architecture (Bronze, Silver, Gold), workflow orchestration with Airflow, and provides a RESTful API for data access.

## Architecture

- **Bronze**: Raw data ingestion from APIs (SEC, MarketStatus, News, Polygon, etc.) and storage in HDFS.
- **Silver**: Data cleaning, normalization, and transformation from Bronze.
- **Gold**: Aggregation and analytical modeling, storing optimized tables for OLAP queries in DuckDB.
- **API**: Flask-based REST API for querying data from the Data Warehouse.
- **Orchestration**: Airflow DAGs automate the entire pipeline.

## Folder Structure

```
Pipeline-Stock-ETL/
│
├── analyse/                # Flask API and scripts for querying the Data Warehouse
├── data/                   # Data storage for bronze, silver, gold layers
├── datawarehouse_config/   # Scripts for initializing Data Warehouse schema
├── etl_datawarehouse/      # ETL processes for Data Warehouse (bronze, silver, gold)
├── etl_source_database/    # ETL for source data (extract, load, transform)
├── schedule/dags/          # Airflow DAGs defining the pipeline
├── .env                    # Configuration, API keys, paths
├── requirements.txt        # Python dependencies
└── README.md               # This documentation

HDFS Structure:
│
├── datalake/
│   └── elt/
│       ├── bronze/
│       │   ├── database_source/
│       │   ├── news/
│       │   └── ohlcs/
│       ├── silver/
│       │   ├── database_source/
│       │   ├── news/
│       │   └── ohlcs/
└── etl_database/
    ├── processed/
    │   ├── transformed_to_database_companies/
    │   ├── transformed_to_database_exchanges/
    │   ├── transformed_to_database_industries/
    │   ├── transformed_to_database_regions/
    │   └── transformed_to_database_sic_industries/
    └── raw/
        ├── companies/
        └── marketstatus/
```

## Main Components

- **ETL Source Database**: Collects data from APIs, stores in HDFS, transforms, and loads into PostgreSQL.
- **ETL Data Warehouse**: Loads and transforms data from Silver to Gold layer in DuckDB.
- **Flask API**: Provides RESTful endpoints to access data from DuckDB.
- **Airflow**: Automates ETL steps via scheduled DAGs.


## Installation Guild
1. Clone the Repository
    ```bash
    git clone https://github.com/yourusername/Pipeline-Stock-ETL.git
    cd Pipeline-Stock-ETL
    ```
2. Configuration
    - **Follow this link**: https://www.notion.so/Configuration-25580772639480408889e150c4367c3d
3. Create a `.env` file in the root directory:
4. Set Up PostgreSQL Database
    ```bash
    # Create database and tables
    psql -U postgres -f /home/tiendat/Pipeline-Stock-ETL/etl_source_database/init_source_database.sql
    ```
5. Initialize DuckDB
    ```bash
    # Create DuckDB database and tables
    python /home/tiendat/Pipeline-Stock-ETL/datawarehouse_config/config.py
    ```
6. Access Airflow Web IU to activate 2 Dags
    - http://localhost:8080

7. Check the data has been updated to Data Warehouse yet
    ```bash
    python /home/tiendat/Pipeline-Stock-ETL/datawarehouse_config/select.py
    ```
## Technologies Used

- Python, Flask, Airflow, DuckDB, PostgreSQL, HDFS, PySpark, Google Cloud Platform, dotenv