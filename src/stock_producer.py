import os
import json
import logging
import pandas as pd
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def create_producer(servers: str):
    # Create Kafka producer with JSON serializer
    return KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

def load_csvs(data_dir: str) -> dict:
    # Load all *_kafka.csv files and group by ticker
    data = {}

    for file in os.listdir(data_dir):
        if not file.endswith("_kafka.csv"):
            continue

        ticker = file.split("_")[0]
        path = os.path.join(data_dir, file)

        df = pd.read_csv(path)
        df["ticker"] = ticker

        logger.info(f"Loaded {len(df)} rows for {ticker} from {file}")
        data[ticker] = df.to_dict(orient="records")

    if not data:
        logger.warning("No CSV files found matching *_kafka.csv")

    return data

def send_to_kafka(data: dict, servers: str):
    # Send all records to Kafka per ticker topic
    producer = create_producer(servers)
    total = 0

    for ticker, records in data.items():
        topic = f"{ticker}_stock_prices"
        count = 0

        for record in records:
            producer.send(topic, key=ticker.encode("utf-8"), value=record)
            count += 1
            total += 1

        logger.info(f"Sent {count} records to topic: {topic}")

    producer.flush()
    logger.info(f"Total {total} records sent for {len(data)} tickers")

def run_producer():
    # Read environment configs
    data_dir = os.getenv("CSV_DIR", "/opt/spark-data/raw/")
    servers = os.getenv("KAFKA_SERVERS", "kafka:9092")

    try:
        logger.info("Starting stock producer")

        data = load_csvs(data_dir)
        if data:
            send_to_kafka(data, servers)

        logger.info("Stock producer completed successfully")

    except Exception as e:
        logger.error(f"Producer error: {e}")
        raise

if __name__ == "__main__":
    run_producer()