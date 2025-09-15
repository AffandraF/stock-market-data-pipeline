# src/flows.py
from prefect import flow, task, get_run_logger
from stock_producer import stock_producer_flow
from utils.spark_runner import run_spark_job
import os
import boto3
from botocore.exceptions import ClientError

# 1. Producer (push to Kafka)
@flow(name="daily-stock-producer")
def scheduled_stock_producer():
    logger = get_run_logger()
    logger.info("Starting stock producer...")
    stock_producer_flow()
    logger.info("Finished stock producer")

# 2. Check if data exists in MinIO/Delta
@task
def check_data(bucket: str, prefix: str) -> bool:
    logger = get_run_logger()
    logger.info(f"Checking if historical data exists in {bucket}/{prefix}...")

    # Init S3 client (MinIO)
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minio"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
    )

    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        exists = "Contents" in resp
        logger.info(f"Historical data exists: {exists}")
        return exists
    except ClientError as e:
        logger.error(f"Failed to check MinIO: {e}")
        return False

# 3. Extract historical data (if not exists)
@flow(name="daily-historical-extractor")
def scheduled_historical_extractor():
    logger = get_run_logger()
    bucket = "stock-data-lake"
    prefix = "raw/"

    exists = check_data(bucket, prefix)

    if exists:
        logger.info("Data already exists, skipping extractor")
        return
    
    logger.info("Running historical extractor...")
    run_spark_job("extract.py")
    logger.info("Finished historical extraction")

# 4. Extract data from Kafka and load to Delta
@flow(name="daily-stock-consumer")
def scheduled_stock_consumer():
    logger = get_run_logger()
    logger.info("Starting stock consumer (Kafka → Delta)...")
    run_spark_job("stock_consumer.py")
    logger.info("Finished stock consumer job")

# 5. Transform data and Load to final table
@flow(name="daily-transform-load")
def scheduled_transform_load():
    logger = get_run_logger()
    logger.info("Starting transform & load job...")
    run_spark_job("transform_load.py")
    logger.info("Finished transform & load job")

# Master flow
@flow(name="daily-stock-etl")
def daily_stock_etl():
    logger = get_run_logger()
    logger.info("Starting daily stock ETL pipeline...")

    # Step 1: Kafka producer
    scheduled_stock_producer()

    # Step 2: Extract historical data (if not exists)
    scheduled_historical_extractor()

    # Step 3: Consume realtime data
    scheduled_stock_consumer()

    # Step 4: Transform & load to Delta/Postgres
    scheduled_transform_load()

    logger.info("🎉 ETL pipeline completed successfully")

# =========================
# Run flows manually
# =========================
# if __name__ == "__main__":
    # Uncomment to test separately
    # print("Running Producer Flow...")
    # scheduled_stock_producer()
    # print("Producer finished\n")

    # print("running Historical Extractor Flow...")
    # scheduled_historical_extractor()
    # print("Extraction finished\n")

    # print("Running Consumer Flow...")
    # scheduled_stock_consumer()
    # print("Consumer finished\n")    

    # print("Running Transform and Load Flow...")
    # scheduled_transform_load()
    # print("Transform and Load finished\n")