import os
from bson import json_util
import time
from kafka import KafkaAdminClient
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient
from prefect import flow, task, get_run_logger

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
DB_NAME = "stock_data"
COLLECTION_NAME = "daily_prices"
MAX_RETRIES = 5
RETRY_DELAY = 5

@task(retries=MAX_RETRIES, retry_delay_seconds=RETRY_DELAY)
def wait_for_kafka():
    """Waits for Kafka to be available."""
    logger = get_run_logger()
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER, request_timeout_ms=5000)
        admin.close()
        logger.info("✅ Kafka is ready")
    except NoBrokersAvailable as e:
        logger.warning(f"⏳ Kafka not ready yet: {e}. Retrying...")
        raise

@task
def consume_from_kafka(max_records: int = 100):
    logger = get_run_logger()
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json_util.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="stock_consumer_" + str(int(time.time())),
        consumer_timeout_ms=10000
    )
    records = []
    for message in consumer:
        records.append(message.value)
        if len(records) >= max_records:
            break
    consumer.close()
    logger.info(f"Fetched {len(records)} records from Kafka")
    return records

@task
def save_raw_to_mongo(records: list):
    """Saves records to MongoDB."""
    logger = get_run_logger()
    if not records:
        logger.warning("No records to save")
        return

    try:
        with MongoClient(MONGO_URI) as client:
            db = client[DB_NAME]
            collection = db[COLLECTION_NAME]
            collection.insert_many(records)
            logger.info(f"Saved {len(records)} raw records to MongoDB")
    except Exception as e:
        logger.error(f"Failed to save to MongoDB: {e}")
        raise


@flow(name="stock-consumer-flow")
def stock_consumer_flow():
    wait_for_kafka()
    raw_records = consume_from_kafka()
    if raw_records:
        save_raw_to_mongo(raw_records)
    else:
        logger = get_run_logger()
        logger.warning("No records received from Kafka, skipping save")

if __name__ == "__main__":
    stock_consumer_flow()