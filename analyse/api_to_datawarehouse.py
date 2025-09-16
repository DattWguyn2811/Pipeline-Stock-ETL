from flask import Flask, jsonify, request
import duckdb
import os 
from dotenv import load_dotenv

load_dotenv()
DATAWAREHOUSE_PATH = os.getenv("DATAWAREHOUSE_PATH")

app = Flask(__name__)

# Define an endpoint to retrieve data from the 'dim_time' table
@app.route('/dim_time', methods=['GET'])
def get_dim_time():
    duck_conn = duckdb.connect(DATAWAREHOUSE_PATH)
    query = "select * from dim_time;"
    result = duck_conn.execute(query).fetchall()
    columns = [desc[0] for desc in duck_conn.description]
    data = [dict(zip(columns, row)) for row in result]
    duck_conn.close()
    return jsonify(data)

# Define an endpoint to retrieve data from the 'dim_companies' table
@app.route('/dim_companies', methods=['GET'])
def get_dim_companies():
    duck_conn = duckdb.connect(DATAWAREHOUSE_PATH)
    query = """
        select 
            company_key,
            company_name,
            company_ticker,
            company_is_delisted,
            company_exchange_name,
            company_industry_name,
            company_industry_sector,
            company_sic_industry,
            company_sic_sector,
            company_update_time_stamp
        from dim_companies;
    """
    result = duck_conn.execute(query).fetchall()
    columns = [desc[0] for desc in duck_conn.description]
    data = [dict(zip(columns, row)) for row in result]
    duck_conn.close()
    return jsonify(data)

# Define an endpoint to retrieve data from the 'dim_topics' table
@app.route('/dim_topics', methods=['GET'])
def get_dim_topics():
    duck_conn = duckdb.connect(DATAWAREHOUSE_PATH)
    query = "select * from dim_topics;"
    result = duck_conn.execute(query).fetchall()
    columns = [desc[0] for desc in duck_conn.description]
    data = [dict(zip(columns, row)) for row in result]
    duck_conn.close()
    return jsonify(data)

# Define an endpoint to retrieve data from the 'dim_news' table
@app.route('/dim_news', methods=['GET'])
def get_dim_news():
    duck_conn = duckdb.connect(DATAWAREHOUSE_PATH)
    query = """
        select 
            news_key,
            news_title,
            news_url,
            news_time_published,
            news_overall_sentiment_score,
            news_overall_sentiment_label,
            news_time_key
        from dim_news;
    """
    result = duck_conn.execute(query).fetchall()
    columns = [desc[0] for desc in duck_conn.description]
    data = [dict(zip(columns, row)) for row in result]
    duck_conn.close()
    return jsonify(data)

# Define an endpoint to retrieve data from the 'fact_news_companies' table
@app.route('/fact_news_companies', methods=['GET'])
def get_fact_news_companies():
    duck_conn = duckdb.connect(DATAWAREHOUSE_PATH)
    query = 'select * from fact_news_companies;'
    result = duck_conn.execute(query).fetchall()
    columns = [desc[0] for desc in duck_conn.description]
    data = [dict(zip(columns, row)) for row in result]
    duck_conn.close()
    return jsonify(data)

# Define an endpoint to retrieve data from the 'fact_news_topics' table
@app.route('/fact_news_topics', methods=['GET'])
def get_fact_news_topics():
    duck_conn = duckdb.connect(DATAWAREHOUSE_PATH)
    query = 'select * from fact_news_topics;'
    result = duck_conn.execute(query).fetchall()
    columns = [desc[0] for desc in duck_conn.description]
    data = [dict(zip(columns, row)) for row in result]
    duck_conn.close()
    return jsonify(data)

# Define an endpoint to retrieve data from the 'fact_candles' table
@app.route('/fact_candles', methods=['GET'])
def get_fact_candles():
    candle_time_key = request.args.get('candle_time_key')
    if not candle_time_key:
        return jsonify({'error': 'candle_time_key parameter is required'}), 400
    try:
        candle_time_key = int(candle_time_key)
    except ValueError:
        return jsonify({'error': 'candle_time_key parameter must be an integer'}), 400
    
    duck_conn = duckdb.connect(DATAWAREHOUSE_PATH)
    query = 'SELECT * FROM fact_candles WHERE candle_time_key = ?;'
    result = duck_conn.execute(query, (candle_time_key,)).fetchall()
    columns = [desc[0] for desc in duck_conn.description]
    data = [dict(zip(columns, row)) for row in result]
    duck_conn.close()
    return jsonify(data)

# Run the application on the specified host and port
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)