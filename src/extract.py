from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

HISTORICAL_CSV = "/opt/spark-data/raw/tlkm_historical_data.csv"
MONGO_URI = "mongodb://mongodb:27017/stock_data.daily_prices"

def extract_historical(spark, ticker: str):
    """Step 1: Extract historical CSV once and save into MongoDB if not already extracted."""
    try:
        last_date = "2025-08-26 00:00:00+07:00"
        existing_df = spark.read.format("mongodb") \
        .option("uri", MONGO_URI) \
        .load() \
        .filter(F.col("ticker") == ticker)

        found_row = existing_df.filter(F.col("date") == F.lit(last_date)).first()
        if found_row is not None:
            print(f"✅ Historical data for ticker='{ticker}' already exists in MongoDB, skipping extraction.")
            return
        
        print("📂 Attempting to extract historical CSV from worker nodes...")
        print(f"--- Spark is attempting to read: '{HISTORICAL_CSV}' ---")

        hist_df = spark.read.csv(HISTORICAL_CSV.strip(), header=True, inferSchema=True)

        # Normalize column names
        hist_df = hist_df.toDF(*[c.lower() for c in hist_df.columns])
        hist_df = hist_df.withColumn("date", F.to_timestamp(F.col("date")))
        hist_df = hist_df.withColumn("ticker", F.lit(ticker)) 

        # Save into MongoDB
        hist_df.write.format("mongodb") \
            .mode("append") \
            .option("uri", MONGO_URI) \
            .save()

        print(f"✅ Historical CSV ingested into MongoDB with ticker='{ticker}'")

    except AnalysisException as e:
        raise e

def extract_data(spark):
    """Step 2: extract combined data from MongoDB"""

    print("📥 Loading combined data from MongoDB...")

    df = spark.read.format("mongodb") \
        .option("uri", MONGO_URI) \
        .load()

    if df is None or df.rdd.isEmpty():
        print("⚠️ No data found in MongoDB, skipping transform")
        return None

    df = df.dropDuplicates(["ticker", "date"])
    df = df.toDF(*[c.lower() for c in df.columns]).drop("_id")
    df = df.withColumn("date", F.to_timestamp(F.col("date")))

    return df