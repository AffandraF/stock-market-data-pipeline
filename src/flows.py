# src/flows.py
from prefect import flow, task, get_run_logger
from stock_producer import run_producer
from utils.spark_runner import run_spark_job
import boto3
from botocore.exceptions import ClientError
import os

# Check if historical data exists in MinIO
@task
def check_data(bucket: str, prefix: str) -> bool:
    logger = get_run_logger()
    logger.info(f"Checking data existence in {bucket}/{prefix}...")

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minio"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
    )

    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        exists = "Contents" in resp
        logger.info(f"Data exists: {exists}")
        return exists
    except ClientError as e:
        logger.error(f"MinIO check failed: {e}")
        return False

# 1. Kafka Producer
@flow(name="daily-stock-producer")
def stock_producer():
    logger = get_run_logger()
    logger.info("Running Kafka producer")
    run_producer()
    logger.info("Producer completed")

# 2. Extract Historical Data (if not exists)
@flow(name="daily-historical-extractor")
def historical_extractor():
    logger = get_run_logger()
    bucket = "stock-data-lake"
    prefix = "raw/"

    if check_data(bucket, prefix):
        logger.info("Historical data already exists, skipping extraction")
        return

    logger.info("Extracting historical data")
    run_spark_job("extract_historical.py")
    logger.info("Historical extraction completed")


# 3. Kafka Consumer → Delta
@flow(name="daily-stock-consumer")
def stock_consumer():
    logger = get_run_logger()
    logger.info("Running Kafka consumer")
    run_spark_job("stock_consumer.py")
    logger.info("Consumer job completed")

# 4. Transform and Load → Delta + Postgres
@flow(name="daily-transform-load")
def transform_load():
    logger = get_run_logger()
    logger.info("Running transform & load job...")
    run_spark_job("transform_load.py")
    logger.info("Transform & load job completed")

# Main ETL flow
@flow(name="daily-stock-etl")
def daily_stock_etl():
    logger = get_run_logger()
    logger.info("Starting daily stock ETL pipeline")

    # Step 1: Produce to Kafka
    stock_producer()

    # Step 2: Extract historical data
    historical_extractor()

    # Step 3: Consume Kafka data to Delta
    stock_consumer()

    # Step 4: Transform and load to Delta/Postgres
    transform_load()

    logger.info("ETL pipeline finished successfully")

# Uncomment below to run manually for debugging
# if __name__ == "__main__":
#     daily_stock_etl()