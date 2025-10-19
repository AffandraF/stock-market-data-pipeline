from pyspark.sql.functions import col, from_json, year, month, to_date
from utils.spark_builder import init_spark
from utils.delta_schema import stock_schema
import logging

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def consume_stream(spark, servers, output_path, checkpoint_path):
    # Read stock data from Kafka
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribePattern", ".*_stock_prices")
        .option("startingOffsets", "earliest")
        .load()
    )

    schema = stock_schema()

    # Parse Kafka JSON messages
    df = (
        df.selectExpr("CAST(key AS STRING) as ticker", "CAST(value AS STRING) as json_str")
          .select(from_json(col("json_str"), schema).alias("data"), col("ticker"))
          .select("data.*", "ticker")
    )

    # Normalize and add partitions
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())

    df = (
        df.withColumn("date", to_date("date", "yyyy-MM-dd"))
          .withColumn("year", year(col("date")))
          .withColumn("month", month(col("date")))
    )

    # Remove duplicates
    df = df.dropDuplicates(["ticker", "date"])

    # Write stream to Delta Lake
    query = (
        df.writeStream
        .format("delta")
        .partitionBy("year", "month")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .start(output_path)
    )
    query.awaitTermination()

def run_consumer(
    servers: str = "kafka:9092",
    output_path: str = "s3a://stock-data-lake/raw/",
    checkpoint_path: str = "s3a://stock-data-lake/checkpoints/stock-consumer/",
):
    try:
        spark = init_spark("StockConsumer")
        consume_stream(spark, servers, output_path, checkpoint_path)
        logger.info("Consumer finished successfully")

    except Exception as e:
        logger.error(f"Error during streaming: {e}")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    run_consumer()
