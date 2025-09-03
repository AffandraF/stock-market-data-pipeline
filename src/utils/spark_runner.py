import os
import subprocess
from prefect import get_run_logger


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