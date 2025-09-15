from kafka import KafkaProducer
import json
import os
import pandas as pd

def get_kafka_producer(bootstrap_servers: str):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

def load_csv_files(data_dir: str) -> dict:
    """Load multiple CSV files and return dict[ticker -> records]"""
    ticker_data = {}

    for file_name in os.listdir(data_dir):
        if not file_name.endswith("_kafka.csv"):
            continue

        ticker = file_name.split("_")[0]
        file_path = os.path.join(data_dir, file_name)

        df = pd.read_csv(file_path)
        df["ticker"] = ticker

        print(f"Loaded {len(df)} rows for ticker {ticker} from {file_name}")
        ticker_data[ticker] = df.to_dict(orient="records")

    if not ticker_data:
        print("No CSV files found matching *_kafka.csv")
    return ticker_data

def push_to_kafka(ticker_data: dict, bootstrap_servers: str):
    """Send records to Kafka, topic per ticker"""
    producer = get_kafka_producer(bootstrap_servers)

    total_count = 0
    for ticker, records in ticker_data.items():
        topic = f"{ticker}_stock_prices"
        count = 0
        for record in records:
            producer.send(topic, key=ticker.encode("utf-8"), value=record)
            count += 1
            total_count += 1

        print(f"Sent {count} records to Kafka topic: {topic}")

    producer.flush()
    print(f"Total {total_count} records sent for {len(ticker_data)} tickers")

def stock_producer_flow(
    data_dir: str = "/opt/spark-data/raw/",
    bootstrap_servers: str = "kafka:9092"
):
    try:
        ticker_data = load_csv_files(data_dir)
        if ticker_data:
            push_to_kafka(ticker_data, bootstrap_servers)
    except Exception as e:  
        print(f"Error in stock producer flow: {e}")
        raise

if __name__ == "__main__":
    stock_producer_flow()