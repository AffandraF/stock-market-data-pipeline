# src/flows.py
from prefect import flow, task, get_run_logger
from stock_producer import stock_producer_flow
from stock_consumer import stock_consumer_flow
import subprocess
import os

# =========================
# Producer Flow
# =========================
@flow(name="daily-stock-producer")
def scheduled_stock_producer():
    stock_producer_flow()

# =========================
# Consumer Flow
# =========================
@flow(name="daily-stock-consumer")
def scheduled_stock_consumer():
    stock_consumer_flow()

# =========================
# Spark Transform Flow
# =========================
@task
def run_spark_job():
    logger = get_run_logger()
    spark_master_url = "spark://spark:7077"
    transform_script = "/opt/prefect/src/transform.py"

    # Pastikan script ada
    if not os.path.exists(transform_script):
        logger.error(f"❌ Transform script not found: {transform_script}")
        raise FileNotFoundError(f"{transform_script} not found")

    try:
        logger.info("🚀 Starting Spark transformation job...")

        result = subprocess.run(
            [
                "spark-submit",
                "--master", spark_master_url,
                "--deploy-mode", "client",
                "--packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.767,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
                transform_script
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

@flow(name="daily-stock-transform")
def scheduled_stock_transform():
    run_spark_job()

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

    print("Running Spark Transform Flow...")
    scheduled_stock_transform()
    print("✅ Transformation finished")
