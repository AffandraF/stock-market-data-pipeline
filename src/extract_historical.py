import os
import glob
import logging
from pyspark.sql.functions import col, year, month, lit, to_date
from utils.delta_schema import stock_schema
from utils.spark_builder import init_spark

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def load_csv(spark, csv_path, output_path):
    # Load a single CSV file and write to Delta
    logger.info(f"Processing {csv_path}...")
    ticker = os.path.basename(csv_path).split("_")[0]
    schema = stock_schema()

    df = (
        spark.read.schema(schema)
        .option("header", True)
        .csv(csv_path)
    )

    # Normalize column names
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())

    # Add metadata columns
    df = (
        df.withColumn("ticker", lit(ticker))
          .withColumn("date", to_date("date", "yyyy-MM-dd"))
          .withColumn("year", year(col("date")))
          .withColumn("month", month(col("date")))
    )

    # Remove duplicates
    df = df.dropDuplicates(["ticker", "date"])

    # Write to Delta Lake
    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("year", "month")
        .save(output_path)
    )

    logger.info(f"Data for {ticker} saved to {output_path}")

def run_extractor(
    csv_dir: str = "/opt/spark-data/raw/",
    output_path: str = "s3a://stock-data-lake/raw/"
):
    # Main extraction flow
    spark = None
    try:
        spark = init_spark("StockExtractor")
        logger.info("Starting extraction...")

        csv_files = glob.glob(os.path.join(csv_dir, "*_history.csv"))
        if not csv_files:
            logger.warning(f"No CSV files found in {csv_dir}")
            return

        for csv_path in csv_files:
            load_csv(spark, csv_path, output_path)

        logger.info("Extraction finished successfully")

    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        raise

    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    run_extractor()