import duckdb

# Kết nối với database
conn = duckdb.connect(database='/home/tiendat/Pipeline-Stock-ETL/datawarehouse.duckdb')

conn.sql("select * from dim_companies;").show()
conn.sql("select count(*) from dim_companies;").show()
conn.sql("select * from dim_time;").show()
conn.sql("select count(*) from dim_time;").show()
conn.sql("select * from dim_news;").show()
conn.sql("select count(*) from dim_news;").show()
conn.sql("select * from dim_topics;").show()
conn.sql("select count(*) from dim_topics;").show()
conn.sql("select * from fact_candles;").show()
conn.sql("select count(*) from fact_candles;").show()
conn.sql("select * from fact_news_companies;").show()
conn.sql("select count(*) from fact_news_companies;").show()
conn.sql("select * from fact_news_topics;").show()
conn.sql("select count(*) from fact_news_topics;").show()