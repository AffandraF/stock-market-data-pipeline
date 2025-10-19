import os
import subprocess
from prefect import get_run_logger

def run_spark_job(script_name: str):
    logger = get_run_logger()
    spark_master = "spark://spark:7077"
    script_path = f"/opt/prefect/src/{script_name}"

    # Check if script exists
    if not os.path.exists(script_path):
        logger.error(f"Spark script not found: {script_path}")
        raise FileNotFoundError(f"{script_path} not found")

    # Required Spark packages
    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "io.delta:delta-spark_2.12:3.2.0",
        "io.delta:delta-storage:3.2.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.postgresql:postgresql:42.7.3"
    ]

    cmd = [
        "spark-submit",
        "--master", spark_master,
        "--deploy-mode", "client",
        "--packages", ",".join(packages),
        script_path
    ]

    try:
        logger.info(f"Running Spark job: {script_name}")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )

        # Log stdout and stderr
        if result.stdout.strip():
            logger.info(result.stdout.strip())
        if result.stderr.strip():
            logger.warning(result.stderr.strip())

        logger.info("Spark job completed successfully")

    except subprocess.CalledProcessError as e:
        logger.error(f"Spark job failed: {script_name}")
        if e.stdout.strip():
            logger.error(e.stdout.strip())
        if e.stderr.strip():
            logger.error(e.stderr.strip())
        raise