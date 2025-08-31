# src/transform.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType
import traceback
import sys
import os

HISTORICAL_CSV = "/opt/spark-data/raw/tlkm_historical_data.csv"
MONGO_URI = "mongodb://mongodb:27017/stock_data.daily_prices"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "stock-data-lake")
S3_TIMEOUT_MS = os.getenv("S3_TIMEOUT_MS", "60000")
OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/processed"

def load_historical(spark, ticker: str):
    """Step 1: Load historical CSV once and save into MongoDB if not already loaded."""
    try:
        print("📂 Attempting to load historical CSV from worker nodes...")
        print(f"--- Spark is attempting to read: '{HISTORICAL_CSV}' ---")

        hist_df = spark.read.csv(HISTORICAL_CSV.strip(), header=True, inferSchema=True)

        # Normalize column names
        hist_df = hist_df.toDF(*[c.lower() for c in hist_df.columns])
        hist_df = hist_df.withColumn("date", F.to_timestamp(F.col("date")))  # overwrite date
        hist_df = hist_df.withColumn("ticker", F.lit(ticker))  # add ticker column

        print("✅ Historical CSV found and loaded by Spark.")
        hist_df.printSchema()

        # Save into MongoDB
        hist_df.write.format("mongodb") \
            .mode("append") \
            .option("uri", MONGO_URI) \
            .save()

        print(f"✅ Historical CSV ingested into MongoDB with ticker='{ticker}'")

    except AnalysisException as e:
        if "Path does not exist" in str(e):
            print(f"⚠️ Historical CSV not found at {HISTORICAL_CSV} on worker nodes, skipping...")
        else:
            raise e

def load_data(spark):
    """Step 2: Load combined data from MongoDB with safe cast for numeric columns."""

    print("📥 Loading combined data from MongoDB...")

    explicit_schema = StructType([
        StructField("_id", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("date", StringType(), True), 
        StructField("open", StringType(), True),
        StructField("high", StringType(), True),
        StructField("low", StringType(), True),
        StructField("close", StringType(), True),
        StructField("volume", StringType(), True) 
    ])

    df = spark.read.format("mongodb") \
        .option("uri", MONGO_URI) \
        .schema(explicit_schema) \
        .load()

    if df is None or df.rdd.isEmpty():
        print("⚠️ No data found in MongoDB, skipping transform")
        return None

    df = df.toDF(*[c.lower() for c in df.columns]).drop("_id")
    df = df.withColumn("date", F.to_timestamp(F.col("date")))

    def safe_cast_long(col):
        return F.when(F.col(col).rlike("^[0-9]+$"), F.col(col).cast("long")).otherwise(None)

    def safe_cast_double(col):
        return F.when(F.col(col).rlike(r"^[0-9]+(\.[0-9]+)?$"), F.col(col).cast("double")).otherwise(None)
    
    df = df.withColumn("volume", safe_cast_long("volume"))
    df = df.withColumn("open", safe_cast_double("open"))
    df = df.withColumn("high", safe_cast_double("high"))
    df = df.withColumn("low", safe_cast_double("low"))
    df = df.withColumn("close", safe_cast_double("close"))

    return df

def transform_data(df):
    """Step 3: Transform with technical indicators."""
    windowSpec = Window.partitionBy("ticker").orderBy("date") if "ticker" in df.columns else Window.orderBy("date")

    print("📊 Starting technical indicators calculation...")

    # SMA
    print("➡️ Calculating SMA (5 & 20)")
    df = df.withColumn("SMA_5", F.avg("close").over(windowSpec.rowsBetween(-4, 0)))
    df = df.withColumn("SMA_20", F.avg("close").over(windowSpec.rowsBetween(-19, 0)))

    # EMA
    print("➡️ Calculating EMA (12)")
    alpha_12 = 2 / (12 + 1)
    df = df.withColumn(
        "EMA_12",
        (1 - alpha_12) * F.lag("close", 1).over(windowSpec) + alpha_12 * F.col("close")
    )

    # RSI
    print("➡️ Calculating RSI (14)")
    df = df.withColumn("prev_close", F.lag("close", 1).over(windowSpec))
    df = df.withColumn("change", F.col("close") - F.col("prev_close"))
    df = df.withColumn("gain", F.when(F.col("change") > 0, F.col("change")).otherwise(0))
    df = df.withColumn("loss", F.when(F.col("change") < 0, -F.col("change")).otherwise(0))

    df = df.withColumn("avg_gain", F.avg("gain").over(windowSpec.rowsBetween(-13, 0)))
    df = df.withColumn("avg_loss", F.avg("loss").over(windowSpec.rowsBetween(-13, 0)))

    # Safe RS
    df = df.withColumn(
        "RS",
        F.when(F.col("avg_loss") == 0, None).otherwise(F.col("avg_gain") / F.col("avg_loss"))
    )

    # Safe RSI
    df = df.withColumn(
        "RSI_14",
        F.when(F.col("RS").isNotNull(), 100 - (100 / (1 + F.col("RS")))).otherwise(100)
    )

    # Bollinger Bands
    print("➡️ Calculating Bollinger Bands (20)")
    df = df.withColumn("rolling_mean", F.avg("close").over(windowSpec.rowsBetween(-19, 0)))
    df = df.withColumn("rolling_std", F.stddev("close").over(windowSpec.rowsBetween(-19, 0)))
    df = df.withColumn("Bollinger_Upper", F.col("rolling_mean") + 2 * F.col("rolling_std"))
    df = df.withColumn("Bollinger_Lower", F.col("rolling_mean") - 2 * F.col("rolling_std"))

    print("✅ Technical indicators calculation completed.")

    return df

def save_data(df):
    """Step 4: Save processed data into Parquet (data lake)."""
    print(f"💾 Saving processed data to {OUTPUT_PATH} in Parquet format...")
    df.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"✅ Processed data saved to {OUTPUT_PATH}")

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
        # load_historical(spark, ticker="TLKM.JK") # assign ticker
        df = load_data(spark)

        if df is not None and not df.rdd.isEmpty():
            df_transformed = transform_data(df)
            save_data(df_transformed)  # pastikan ini memicu aksi (misalnya write)

    except Exception as e:
        print("❌ Error in pipeline")
        print(str(e))
        traceback.print_exc(file=sys.stdout)

    finally:
        # ✅ stop SparkContext hanya sekali, paling akhir
        spark.stop()


if __name__ == "__main__":
    main()
