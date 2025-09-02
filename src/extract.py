from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
from prefect import flow, task, get_run_logger
from utils.spark_builder import init_spark

# Task: load historical CSV
@task
def load_historical_csv(spark, csv_path, output_path):
    logger = get_run_logger()

    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    df = df.withColumn("date", col("date").cast("date"))

    # Add partition columns
    df = df.withColumn("year", year(col("date"))) \
           .withColumn("month", month(col("date")))

    # Save as Delta with partitioning
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .partitionBy("year", "month") \
      .save(output_path)

    logger.info(f"✅ Historical data saved to {output_path} partitioned by year, month")

# Flow: Historical Loader
@flow(name="daily-historical-loader")
def extract_historical_flow(
    csv_path: str = "data/raw/historical.csv",
    output_path: str = "s3a://stock-data/raw/"
):
    spark = init_spark("StockExtract")
    load_historical_csv(spark, csv_path, output_path)
    spark.stop()

if __name__ == "__main__":
    extract_historical_flow()
