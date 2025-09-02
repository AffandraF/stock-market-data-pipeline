from pymongo import MongoClient
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

HISTORICAL_CSV = "/opt/spark-data/raw/tlkm_historical_data.csv"
MONGO_CLIENT_URI = "mongodb://mongodb:27017/"
MONGO_URI = "mongodb://mongodb:27017/stock_data.daily_prices" 

def extract_historical(spark, ticker: str):
    """Step 1: Extract historical CSV once and save into MongoDB if not already extracted."""
    client = None
    try:
        client = MongoClient(MONGO_CLIENT_URI)
        collection = client.stock_data.daily_prices
        if collection.count_documents({'ticker': ticker}) > 0:
            print(f"✅ Historical data for ticker='{ticker}' already exists. Skipping Spark job.")
            return
    except Exception as e:
        print(f"⚠️ Could not connect to MongoDB: {e}")
    finally:
        if client:
            client.close()
    
    try:        
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

        print(f"Historical CSV ingested into MongoDB with ticker='{ticker}'")

    except AnalysisException as e:
        raise e
    
def extract_data(spark, ticker: str):
    """Get all data from MongoDB."""
    query = f"{{'$match': {{'ticker': '{ticker}'}}}}"
    
    mongo_df = spark.read.format("mongodb") \
        .option("uri", MONGO_URI) \
        .option("pipeline", query) \
        .load()

    if mongo_df.isEmpty():
        raise ValueError(f"No data found for ticker='{ticker}' in MongoDB after historical check.")

    last_date = mongo_df.agg(F.max("date")).collect()[0][0]
    print(f"Last date in Mongo for {ticker}: {last_date}")

    return mongo_df, last_date