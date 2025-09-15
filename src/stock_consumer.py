from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import year, month, to_date
from utils.spark_builder import init_spark
from utils.delta_schema import stock_schema

def consume_and_save(spark, bootstrap_servers, minio_path, checkpoint_path, timeout=None):
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribePattern", ".*_stock_prices")
        .option("startingOffsets", "earliest")
        .load()
    )

    schema = stock_schema()
    # Parse Kafka message
    df = (
    df.selectExpr(
        "CAST(key AS STRING) as ticker",
        "CAST(value AS STRING) as json_str"
    )
        .select(from_json(col("json_str"), schema).alias("data"), col("ticker"))
        .select("data.*", "ticker")
    )
    
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())

    df = (
        df.withColumn("date", to_date("date", "yyyy-MM-dd"))
          .withColumn("year", year(col("date")))
          .withColumn("month", month(col("date")))
    )

    # Write to Delta Lake (MinIO/S3)
    query = (
        df.writeStream
        .format("delta")
        .partitionBy("year", "month")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .start(minio_path)
    )

    if timeout:
        print(f"Running streaming query for {timeout} seconds...")
        query.awaitTermination(timeout)
        query.stop()
        spark.stop()
        print("Batch simulation finished and Spark stopped")
    else:
        print("Running streaming query ...")
        query.awaitTermination()

def stock_consumer_flow(
    bootstrap_servers: str = "kafka:9092",
    minio_path: str = "s3a://stock-data-lake/raw/",
    checkpoint_path: str = "s3a://stock-data-lake/checkpoints/stock-consumer/",
    timeout: int = 20  # None for infinite streaming mode
):
    try:
        spark = init_spark("StockConsumer")
        consume_and_save(spark, bootstrap_servers, minio_path, checkpoint_path, timeout)

        print("Stock consumer flow completed successfully")
    
    except Exception as e:
        print(f"Error during extraction: {e}")
        raise

    finally:
        spark.stop()
        print("Spark session stopped")

if __name__ == "__main__":
    stock_consumer_flow()