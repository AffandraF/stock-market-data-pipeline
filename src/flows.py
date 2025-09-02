# src/flows.py
from prefect import flow, task, get_run_logger
from stock_producer import stock_producer_flow
from stock_consumer import stock_consumer_flow
from extract import extract_historical_flow
from transform_load import transform_load_flow
import subprocess
import os

# 1. Producer (push ke Kafka)
@flow(name="daily-stock-producer")
def scheduled_stock_producer():
    stock_producer_flow()

# 2. Consumer (ambil dari Kafka + simpan Bronze ke MinIO)
@flow(name="daily-stock-consumer")
def scheduled_stock_consumer():
    stock_consumer_flow()

# 3. Load historical CSV
@flow(name="daily-historical-extractor")
def scheduled_historical_extractor():
    extract_historical_flow()

# 4. Gold Layer (transformasi teknikal)
@flow(name="daily-transform-load")
def scheduled_transform_load():
    transform_load_flow()


@task
def run_spark_job(script_name: str):
    logger = get_run_logger()
    spark_master_url = "spark://spark:7077"
    script_path = f"/opt/prefect/src/{script_name}"

    # Pastikan script ada
    if not os.path.exists(script_path):
        logger.error(f"❌ Transform script not found: {script_path}")
        raise FileNotFoundError(f"{script_path} not found")

    try:
        logger.info("🚀 Starting Spark transformation job...")

        result = subprocess.run(
            [
                "spark-submit",
                "--master", spark_master_url,
                "--deploy-mode", "client",
                "--packages",
                (
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.767,"
                    "io.delta:delta-core_2.12:2.4.0,"
                    "io.delta:delta-storage:2.4.0,"
                    "org.postgresql:postgresql:42.7.3,"
                    "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
                ),
                script_path
            ],
            capture_output=True,
            text=True,
            check=True
        )

        logger.info("✅ Spark job finished successfully")
        if result.stdout:
            logger.info(result.stdout)
        if result.stderr:
            logger.warning(result.stderr)

    except subprocess.CalledProcessError as e:
        logger.error("❌ Spark job failed")
        logger.error(e.stderr)
        raise

# =========================
# Run flows manually
# =========================
if __name__ == "__main__":
    # Uncomment if you want to test producer/consumer separately
    # print("Running Producer Flow...")
    # scheduled_stock_producer()
    # print("✅ Producer finished\n")

    # print("Running Consumer Flow...")
    # scheduled_stock_consumer()
    # print("✅ Consumer finished\n")

    print("running Historical Extractor Flow...")
    scheduled_historical_extractor()
    print("✅ Extraction finished\n")

    print("Running Transform and Load Flow...")
    scheduled_transform_load()
    print("✅ Transform and Load finished\n")
