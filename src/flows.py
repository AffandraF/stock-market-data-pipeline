# src/flows.py
from prefect import flow, task, get_run_logger
from stock_producer import stock_producer_flow
from spark_runner import run_spark_job
import os
import boto3
from botocore.exceptions import ClientError

# 1. Producer (push ke Kafka)
@flow(name="daily-stock-producer")
def scheduled_stock_producer():
    stock_producer_flow()

# 2. Check if data exists in MinIO/Delta
@task
def check_data(bucket: str, prefix: str) -> bool:
    """Check if historical data already exists in MinIO/Delta"""
    logger = get_run_logger()

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
        logger.info(f"📂 Historical data exists: {exists}")
        return exists
    except ClientError as e:
        logger.error(f"❌ Failed to check MinIO: {e}")
        return False
    
# 3. Load historical CSV
@flow(name="daily-historical-extractor")
def scheduled_historical_extractor():
    bucket = "stock-data"
    prefix = "raw/"

    exists = check_data(bucket, prefix)

    if exists:
        get_run_logger().info("✅ Data already exists, skipping extractor")
        return
    
    run_spark_job("extract_historical.py")

# 4. Consumer (ambil dari Kafka + simpan Bronze ke MinIO)
@flow(name="daily-stock-consumer")
def scheduled_stock_consumer():
    run_spark_job("stock_consumer.py")

# 4. Gold Layer (transformasi teknikal)
@flow(name="daily-transform-load")
def scheduled_transform_load():
    run_spark_job("transform_load.py")

# =========================
# Run flows manually
# =========================
if __name__ == "__main__":
    # Uncomment if you want to test producer/consumer separately
    print("Running Producer Flow...")
    scheduled_stock_producer()
    print("✅ Producer finished\n")

    print("running Historical Extractor Flow...")
    scheduled_historical_extractor()
    print("✅ Extraction finished\n")

    print("Running Consumer Flow...")
    scheduled_stock_consumer()
    print("✅ Consumer finished\n")    

    print("Running Transform and Load Flow...")
    scheduled_transform_load()
    print("✅ Transform and Load finished\n")
