from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from prefect import flow, task, get_run_logger
from utils.spark_builder import init_spark

# Define schema for stock data
stock_schema = StructType([
    StructField("date", StringType(), True),
    StructField("ticker", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
])

@task
def consume_and_save(spark, topic, bootstrap_servers, minio_path, checkpoint_path, timeout=None):
    logger = get_run_logger()

    # Read from Kafka
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse Kafka value to JSON
    value_df = df.selectExpr("CAST(value AS STRING) as json_str")
    parsed_df = value_df.select(from_json(col("json_str"), stock_schema).alias("data")).select("data.*")

    # Write to Delta Lake (MinIO/S3)
    query = (
        parsed_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start(minio_path)
    )

    if timeout:
        logger.info(f"Running streaming query for {timeout} seconds...")
        query.awaitTermination(timeout)
        query.stop()
        spark.stop()
        logger.info("✅ Batch simulation finished and Spark stopped")
    else:
        logger.info("Running streaming query ...")
        query.awaitTermination()

@flow(name="stock-consumer-flow")
def stock_consumer_flow(
    topic: str = "stock_prices",
    bootstrap_servers: str = "localhost:9092",
    minio_path: str = "s3a://stock-data/raw/",
    checkpoint_path: str = "s3a://stock-data/checkpoints/stock-consumer/",
    timeout: int = 20  # None for infinite streaming mode
):
    spark = init_spark("StockConsumer")
    consume_and_save(spark, topic, bootstrap_servers, minio_path, checkpoint_path, timeout)
    spark.stop()

if __name__ == "__main__":
    stock_consumer_flow()