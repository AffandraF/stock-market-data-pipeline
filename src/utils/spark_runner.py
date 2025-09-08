import os
import subprocess
from prefect import get_run_logger

def run_spark_job(script_name: str):
    logger = get_run_logger()
    spark_master_url = "spark://spark:7077"
    script_path = f"/opt/prefect/src/{script_name}"

    # Check if script exists
    if not os.path.exists(script_path):
        logger.error(f"❌ Spark script not found: {script_path}")
        raise FileNotFoundError(f"{script_path} not found")

    # List of compatible packages
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "io.delta:delta-spark_2.12:3.2.0",
        "io.delta:delta-storage:3.2.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.postgresql:postgresql:42.7.3"
    ]

    try:
        logger.info(f"🚀 Starting Spark job: {script_name}")

        result = subprocess.run(
            [
                "spark-submit",
                "--master", spark_master_url,
                "--deploy-mode", "client",
                "--packages", ",".join(packages),
                script_path
            ],
            capture_output=True,
            text=True,
            check=True
        )

        if result.stdout:
            logger.info(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"STDERR:\n{result.stderr}")

        logger.info("✅ Spark job finished successfully")

    except subprocess.CalledProcessError as e:
        logger.error("❌ Spark job failed")
        if e.stdout:
            logger.error(f"STDOUT:\n{e.stdout}")
        if e.stderr:
            logger.error(f"STDERR:\n{e.stderr}")
        raise