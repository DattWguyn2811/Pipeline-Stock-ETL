import logging
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os

# Load .env
load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

USER = "tiendat"

# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("Transform and load data to Silver layer") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()


def read_bronze_data(partition_date: str):
    """Read data from Bronze layer."""
    hdfs_path = f"/user/{USER}/datalake/elt/bronze/news/dt={partition_date}"
    try:
        df = spark.read.parquet(hdfs_path)
        logging.info(f"Read {df.count()} rows from Bronze layer")
        return df
    except Exception as e:
        logging.error(f"Failed to read data from Bronze layer: {e}")
        return None

def transform_data(df):
    """Transform data for Silver layer."""
    if df is None:
        logging.warning("No data to transform")
        return None
    
    df = df.drop("banner_image", "source_domain", "category_within_source")
    
    rename_cols = {
        "title" : "news_title",
        "url" : "news_url",
        "time_published" : "news_time_published",
        "authors" : "news_authors",
        "summary" : "news_summary",
        "source" : "news_source", 
        "overall_sentiment_score" : "news_overall_sentiment_score",
        "overall_sentiment_label" : "news_overall_sentiment_label"
    }

    for old_col, new_col in rename_cols.items():
        df = df.withColumnRenamed(old_col, new_col)

    df = df.withColumn("tickers", F.explode_outer("ticker_sentiment")) \
              .withColumn("news_ticker", F.col("tickers.ticker")) \
              .withColumn("news_company_relevance_score", F.col("tickers.relevance_score")) \
              .withColumn("news_company_ticker_sentiment_score", F.col("tickers.ticker_sentiment_score")) \
              .withColumn("news_company_ticker_sentiment_label", F.col("tickers.ticker_sentiment_label")) \
              .drop("tickers", "ticker_sentiment")

    df = df.withColumn("topic", F.explode_outer("topics")) \
              .withColumn("topic_name", F.col("topic.topic")) \
              .withColumn("news_topic_relevance_score", F.col("topic.relevance_score")) \
              .drop("topics", "topic")

    df = df.withColumn("news_time_published", F.to_timestamp("news_time_published", "yyyyMMdd'T'HHmmss"))

    string_cols = [
        "news_title", "news_url", 
        "news_time_published", 
        "news_summary", 
        "news_source",  
        "news_overall_sentiment_label", 
        "news_ticker", 
        "news_company_ticker_sentiment_label", 
        "topic_name"
    ]

    double_cols = [
        "news_overall_sentiment_score", 
        "news_company_relevance_score",
        "news_company_ticker_sentiment_score", 
        "news_topic_relevance_score"
    ]

    for col in string_cols:
        df = df.withColumn(col, F.coalesce(F.trim(F.col(col)).cast("string"), F.lit("n/a")))

    for col in double_cols:
        df = df.withColumn(
            col,
            F.when(F.col(col).cast("double") < 0, F.lit(0.0))  
            .otherwise(F.coalesce(F.col(col).cast("double"), F.lit(0.0)))  
        )

    df = df.dropDuplicates()

    logging.info(f"Transformed data: {df.count()} rows")
    return df


def save_to_silver(df, partition_date: str):
    """Save transformed data to Silver layer."""
    hdfs_path = f"/user/{USER}/datalake/elt/silver/news/dt={partition_date}"
    try:
        df.write.mode("overwrite").parquet(hdfs_path)
        logging.info(f"Saved {df.count()} rows to Silver layer at {hdfs_path}")
    except Exception as e:
        logging.error(f"Failed to save to Silver layer: {e}")


def transform_bronze_to_silver():
    partition_date = datetime.date.today() - datetime.timedelta(days=1)
    df = read_bronze_data(partition_date)
    if df:
        transformed_df = transform_data(df)
        if transformed_df:
            save_to_silver(transformed_df, partition_date)


if __name__ == "__main__":
    transform_bronze_to_silver()