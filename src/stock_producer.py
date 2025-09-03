from kafka import KafkaProducer
import json
import os
import pandas as pd
from prefect import flow, task, get_run_logger

def get_kafka_producer(bootstrap_servers: str):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

@task
def load_csv_files(data_dir: str) -> dict:
    """Load multiple CSV files and return dict[ticker -> records]"""
    logger = get_run_logger()
    ticker_data = {}

    for file_name in os.listdir(data_dir):
        if not file_name.endswith("_kafka.csv"):
            continue

        ticker = file_name.split("_")[0]
        file_path = os.path.join(data_dir, file_name)

        df = pd.read_csv(file_path)
        df["ticker"] = ticker

        logger.info(f"📂 Loaded {len(df)} rows for ticker {ticker} from {file_name}")
        ticker_data[ticker] = df.to_dict(orient="records")

    if not ticker_data:
        logger.warning("⚠️ No CSV files found matching *_kafka.csv")
    return ticker_data

@task
def push_to_kafka(ticker_data: dict, bootstrap_servers: str):
    """Send records to Kafka, topic per ticker"""
    logger = get_run_logger()
    producer = get_kafka_producer(bootstrap_servers)

    total_count = 0
    for ticker, records in ticker_data.items():
        topic = f"{ticker}_stock_prices"
        count = 0
        for record in records:
            producer.send(topic, key=ticker.encode("utf-8"), value=record)
            count += 1
            total_count += 1

        logger.info(f"✅ Sent {count} records to Kafka topic: {topic}")

    producer.flush()
    logger.info(f"🎯 Total {total_count} records sent for {len(ticker_data)} tickers")


@flow(name="stock-producer-flow")
def stock_producer_flow(
    data_dir: str = "data/raw/",
    bootstrap_servers: str = "localhost:9092"
):
    ticker_data = load_csv_files(data_dir)
    if ticker_data:
        push_to_kafka(ticker_data, bootstrap_servers)

if __name__ == "__main__":
    stock_producer_flow()