from pyspark.sql import SparkSession
from src.extract import extract_historical, extract_data
from src.transform import transform_data
from src.load import load_to_s3
import traceback
import sys
import os

MONGO_URI = "mongodb://mongodb:27017/stock_data.daily_prices"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

def main():
    spark = (
        SparkSession.builder
        .appName("StockTransform")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    try:
        extract_historical(spark, ticker="TLKM.JK") # assign ticker
        df = extract_data(spark)

        if df is not None and not df.rdd.isEmpty():
            df_transformed = transform_data(df)
            load_to_s3(df_transformed)

    except Exception as e:
        print("❌ Error in pipeline")
        print(str(e))
        traceback.print_exc(file=sys.stdout)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()