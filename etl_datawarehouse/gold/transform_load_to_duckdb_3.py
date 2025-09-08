import logging
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os
import duckdb
import pyarrow as pa

# Load .env
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

DATAWAREHOUSE_PATH = os.getenv("DATAWAREHOUSE_PATH")
USER = "tiendat"

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Transform and load data to Gold layer") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()


def read_silver_data(partition_date: str):
    """Read ohlcs data from Silver layer."""
    hdfs_path = f"/user/{USER}/datalake/elt/silver/news/dt={partition_date}"
    try:
        logging.info(f"Reading Silver data from {hdfs_path}")
        df = spark.read.parquet(hdfs_path)
        row_count = df.count()
        logging.info(f"Read {row_count} rows from Silver layer for partition {partition_date}")
        return df
    except Exception as e:
        logging.error(f"Failed to read data from Silver layer: {e}")
        return None


def process_data(df):
    """Processing ohlcs data for Gold layer."""
    if df is None:
        logging.warning("No data to transform. Skipping process.")
        return None   

    database_path = os.getenv("DATAWAREHOUSE_PATH")
    conn = duckdb.connect(database=database_path) 

    topics_df = df.select("topic_name").distinct()
    arrow_table_topics = pa.Table.from_pandas(topics_df.toPandas())
    conn.register('arrow_table_topics', arrow_table_topics)
    conn.execute(
        """
        insert into dim_topics (topic_name)
        select * from arrow_table_topics
        where topic_name not in (
            select topic_name from dim_topics
        );"""
    )

    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    conn.execute(
        f"""insert into dim_time (
            time_date,
            time_day_of_week,
            time_month,
            time_quarter,
            time_year
        ) 
        select
            '{yesterday}',
            '{yesterday.strftime("%A")}',
            '{yesterday.strftime("%B")}',
            '{((yesterday.month - 1) // 3) + 1}',
            {yesterday.year}
        where not exists (
            select 1 from dim_time where time_date = '{yesterday}' 
        );"""
    )
    time_key_df = conn.execute(
        f"""
        select time_key from dim_time where time_date = '{yesterday}'                                
        """
    ).fetchdf()
    news_time_key = time_key_df['time_key'][0]

    news_df = df.select(
        F.col("news_title"),
        F.col("news_url"),
        F.col("news_time_published"),
        F.col("news_authors"),
        F.col("news_summary"),
        F.col("news_source"),
        F.col("news_overall_sentiment_score"),
        F.col("news_overall_sentiment_label")
    ).withColumn("news_time_key", F.lit(news_time_key))

    arrow_table_news = pa.Table.from_pandas(news_df.toPandas())
    conn.register("arrow_table_news", arrow_table_news)

    conn.execute(
        """insert into dim_news (
            news_title,
            news_url,
            news_time_published,
            news_authors,
            news_summary,
            news_source,
            news_overall_sentiment_score,
            news_overall_sentiment_label,
            news_time_key
        ) 
        select 
            news_title,
            news_url,
            news_time_published,
            news_authors,
            news_summary,
            news_source,
            news_overall_sentiment_score,
            news_overall_sentiment_label,
            news_time_key
        from arrow_table_news;"""
    )

    fact_news_topics_df = df.select(
        F.col("news_topic_relevance_score"),
        F.col("topic_name"),
        F.col("news_title")
    )
    arrow_table_fact_news_topics = pa.Table.from_pandas(fact_news_topics_df.toPandas())
    conn.register("arrow_table_fact_news_topics", arrow_table_fact_news_topics)
    conn.execute(
        """
        insert into fact_news_topics (
            news_topic_news_key,
            news_topic_topic_key,
            news_topic_relevance_score
        )
        select 
            dn.news_key as news_topic_news_key,
            dt.topic_key as news_topic_topic_key,
            fnt.news_topic_relevance_score
        from arrow_table_fact_news_topics fnt
        left join dim_topics dt
        on fnt.topic_name = dt.topic_name
        left join dim_news dn
        on fnt.news_title = dn.news_title
        where dn.news_key is not null or dt.topic_key is not null;
        """
    )

    fact_news_companies_df = df.select(
        F.col("news_company_relevance_score"),
        F.col("news_ticker"),
        F.col("news_company_ticker_sentiment_score"),
        F.col("news_company_ticker_sentiment_label"),
        F.col("news_title")
    )

    arrow_table_fact_news_companies = pa.Table.from_pandas(fact_news_companies_df.toPandas())
    conn.register("arrow_table_fact_news_companies", arrow_table_fact_news_companies)
    conn.execute(
        """
        insert into fact_news_companies (
            news_company_company_key,
            news_company_news_key,
            news_company_relevance_score,
            news_company_ticker_sentiment_score,
            news_company_ticker_sentiment_label
        )
        select 
            dc.company_key,
            dn.news_key,
            fnc.news_company_relevance_score,
            fnc.news_company_ticker_sentiment_score,
            fnc.news_company_ticker_sentiment_label
        from arrow_table_fact_news_companies fnc
        left join dim_news dn 
        on dn.news_title = fnc.news_title
        left join (
            select
                t.company_key,
                t.company_ticker,
                t.company_update_time_stamp
            from (
                select 
                    company_key, 
                    company_ticker, 
                    company_update_time_stamp, 
                    row_number() over (partition by company_ticker order by company_update_time_stamp desc) as row_num
                from dim_companies) t
            where t.row_num = 1
        ) dc
        on dc.company_ticker = fnc.news_ticker
        where company_key is not null 
        and news_key is not null;
        """
    )
    conn.close()

def transform_load_to_duckdb():
    partition_date = datetime.date.today() - datetime.timedelta(days=1)

    df = read_silver_data(partition_date)
    if df:
        process_data(df)
    else:
        logging.warning("No data returned from Silver layer. Nothing loaded to DuckDB.")


if __name__ == "__main__":    
    transform_load_to_duckdb()