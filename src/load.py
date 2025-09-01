import os
from pyspark.sql import functions as F

MINIO_BUCKET = os.getenv("MINIO_BUCKET", "stock-data-lake")
OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/processed"

def load_to_s3(df, ticker: str):
    """Save transformed data to MinIO/S3 in Delta format (partitioned)."""
    df = df.withColumn("year", F.year("date")).withColumn("month", F.month("date"))

    df.write.format("delta") \
        .mode("append") \
        .partitionBy("ticker", "year", "month") \
        .save(OUTPUT_PATH)

    print(f"✅ Data for {ticker} saved to MinIO/S3.")