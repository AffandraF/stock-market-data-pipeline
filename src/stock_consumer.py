from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import year, month, regexp_extract
from utils.spark_builder import init_spark

stock_schema = StructType([
    StructField("date", StringType(), True),
    StructField("ticker", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
])

def consume_and_save(spark, bootstrap_servers, minio_path, checkpoint_path, timeout=None):

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribePattern", ".*_stock_prices")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse Kafka value to JSON
    value_df = df.selectExpr("CAST(value AS STRING) as json_str", "topic")
    parsed_df = (
        value_df
        .select(from_json(col("json_str"), stock_schema).alias("data"), col("topic"))
        .select("data.*", "topic")
    )

    parsed_df = parsed_df.withColumn("ticker", regexp_extract(col("topic"), r"^([A-Z]+)_stock_prices$", 1))

    parsed_df = parsed_df.withColumn("date", col("date").cast("date"))

    parsed_df = (
        parsed_df
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
    )

    # Write to Delta Lake (MinIO/S3)
    query = (
        parsed_df.writeStream
        .format("delta")
        .partitionBy("ticker", "year", "month")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start(minio_path)
    )

    if timeout:
        print(f"Running streaming query for {timeout} seconds...")
        query.awaitTermination(timeout)
        query.stop()
        spark.stop()
        print("✅ Batch simulation finished and Spark stopped")
    else:
        print("Running streaming query ...")
        query.awaitTermination()

def stock_consumer_flow(
    bootstrap_servers: str = "localhost:9092",
    minio_path: str = "s3a://stock-data/raw/",
    checkpoint_path: str = "s3a://stock-data/checkpoints/stock-consumer/",
    timeout: int = 20  # None for infinite streaming mode
):
    spark = init_spark("StockConsumer")
    consume_and_save(spark, bootstrap_servers, minio_path, checkpoint_path, timeout)
    spark.stop()


if __name__ == "__main__":
    stock_consumer_flow()