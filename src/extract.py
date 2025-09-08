from pyspark.sql.functions import col, year, month, lit, to_date
from utils.delta_schema import stock_schema
import os
import glob
from utils.spark_builder import init_spark

def load_historical_csv(spark, csv_path, output_path):

    print(f"Processing {csv_path}...")

    ticker = os.path.basename(csv_path).split("_")[0] 
    
    schema = stock_schema()
    df = (
        spark.read.schema(schema)
        .option("header", True)
        .csv(csv_path)
    )

    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower())

    df = (
        df.withColumn("ticker", lit(ticker))
          .withColumn("date", to_date("date", "yyyy-MM-dd"))
          .withColumn("year", year(col("date")))
          .withColumn("month", month(col("date")))
    )

    # Write to Delta Lake (MinIO/S3)
    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("year", "month")
        .save(output_path)
    )

    print(f"✅ Historical data for {ticker} saved to {output_path}")

def extract_historical_flow(
    csv_dir: str = "/opt/spark-data/raw/",
    output_path: str = "s3a://stock-data-lake/raw/"
):
    try:
        spark = init_spark("StockExtract")

        print("Starting historical data extraction...")
        for csv_path in glob.glob(os.path.join(csv_dir, "*_history.csv")):
            load_historical_csv(spark, csv_path, output_path)

        print("✅ Extraction completed successfully")

    except Exception as e:
        print(f"Error during extraction: {e}")
        raise

    finally:
        spark.stop()
        print("Spark session stopped")

if __name__ == "__main__":
    extract_historical_flow()
