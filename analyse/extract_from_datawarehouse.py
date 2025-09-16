import requests
import json
import os

# Các endpoint
BASE_URL = "http://127.0.0.1:5000"
ENDPOINTS = {
    "dim_time": f"{BASE_URL}/dim_time",
    "dim_companies": f"{BASE_URL}/dim_companies",
    "dim_topics": f"{BASE_URL}/dim_topics",
    "dim_news": f"{BASE_URL}/dim_news",
    "fact_news_companies": f"{BASE_URL}/fact_news_companies",
    "fact_news_topics": f"{BASE_URL}/fact_news_topics",
    "fact_candles": f"{BASE_URL}/fact_candles?candle_time_key=1",
}

# Thư mục lưu file
OUTPUT_DIR = "/home/tiendat/Pipeline-Stock-ETL/data/gold"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def download_and_save(name, url):
    try:
        print(f"Fetching {name} from {url} ...")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        output_path = os.path.join(OUTPUT_DIR, f"{name}.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"✅ Saved {name}.json")
    except Exception as e:
        print(f"❌ Error fetching {name}: {e}")

def main():
    for name, url in ENDPOINTS.items():
        download_and_save(name, url)

if __name__ == "__main__":
    main()