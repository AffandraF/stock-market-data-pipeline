from kafka import KafkaProducer
import json
import os
import pandas as pd
from prefect import flow, task, get_run_logger

def get_kafka_producer(bootstrap_servers: str):
    """Create Kafka producer with JSON serializer"""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

@task
def load_weekly_data(file_path: str):
    """Load weekly stock data for multiple tickers"""
    logger = get_run_logger()
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(file_path)
    
    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
    elif file_path.endswith(".json"):
        df = pd.read_json(file_path)
    else:
        raise ValueError("Unsupported file format (only csv/json allowed)")
    
    if "ticker" not in df.columns:
        raise ValueError("CSV/JSON must have 'ticker' column")
    
    logger.info(f"Loaded {len(df)} records, {df['ticker'].nunique()} tickers from {file_path}")
    return df.to_dict(orient="records")


@task
def push_to_kafka(records: list, topic: str, bootstrap_servers: str):
    """Send records to Kafka, grouped by ticker"""
    logger = get_run_logger()
    producer = get_kafka_producer(bootstrap_servers)

    count = 0
    for record in records:
        ticker = record.get("ticker", "UNKNOWN")
        producer.send(topic, key=ticker.encode("utf-8"), value=record)
        count += 1
    
    producer.flush()
    logger.info(f"✅ Sent {count} records to Kafka topic: {topic}")

@flow(name="stock-producer-flow")
def stock_producer_flow(
    file_path: str = "data/yfinance_weekly.csv",
    topic: str = "stock_prices",
    bootstrap_servers: str = "localhost:9092"
):
    """Prefect flow for producing stock data into Kafka"""
    records = load_weekly_data(file_path)
    push_to_kafka(records, topic, bootstrap_servers)

if __name__ == "__main__":
    stock_producer_flow()