from pyspark.sql import SparkSession
from pyspark.sql import SparkSession, functions as F
from extract import extract_historical, extract_data
from src.transform_load import transform_data
from load import load_to_s3
import os

MONGO_URI = "mongodb://mongodb:27017/stock_data.daily_prices"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

def create_spark():
    return SparkSession.builder \
        .appName("StockTransform") \
        .config("spark.mongodb.read.connection.uri", MONGO_URI) \
        .config("spark.mongodb.write.connection.uri", MONGO_URI) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def process_ticker(spark, ticker: str):
    """
    Process a single ticker: extract, transform, load.
    """
    try:
        # Step 1: Extract historical once
        extract_historical(spark, ticker)

        # Step 2: Extract incremental from Mongo
        mongo_df, last_date = extract_data(spark, ticker)

        # Step 3: Load gold layer
        try:
            gold_df = spark.read.format("delta").load("s3a://stock-data/processed/")
            last_gold = gold_df.filter(F.col("ticker") == ticker).agg(F.max("date")).collect()[0][0]
        except Exception:
            gold_df = None
            last_gold = None

        # Step 4: Combine last window + new data
        if gold_df and last_gold:
            window_df = gold_df.filter(F.col("ticker") == ticker) \
                       .orderBy(F.col("date").desc()) \
                       .limit(20)
            new_data = mongo_df.filter(F.col("date") > last_gold)
            
            if new_data.isEmpty():
                print(f"⚠️ No new data for {ticker}. Skipping.")
                return
            combined = window_df.union(new_data).orderBy("date")
        else:
            combined = mongo_df.filter(F.col("date") <= last_date).orderBy("date")

        if combined.count() == 0:
            print(f"⚠️ No new data for {ticker}. Skipping.")
            return

        # Step 5: Transform
        transformed = transform_data(combined)

        # Step 6: Keep only new rows (if gold exists)
        if last_gold:
            transformed = transformed.filter(F.col("date") > last_gold)

        # Step 7: Load to S3
        load_to_s3(transformed, ticker)

        print(f"✅ Pipeline for {ticker} completed successfully.")

    except Exception as e:
        print(f"❌ Error in pipeline for {ticker}: {e}")


def run_pipeline(tickers: list):
    """
    Run the pipeline for multiple tickers.
    """
    spark = create_spark()
    try:
        for ticker in tickers:
            process_ticker(spark, ticker)
    finally:
        spark.stop()

if __name__ == "__main__":
    tickers = ["TLKM.JK"]
    run_pipeline(tickers)