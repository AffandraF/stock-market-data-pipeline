from pyspark.sql.functions import col, year, month, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os
import glob
from utils.spark_builder import init_spark

stock_schema = StructType([
    StructField("date", StringType(), True),
    StructField("ticker", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
])

def load_historical_csv(spark, csv_path, output_path):

    ticker = os.path.basename(csv_path).split("_")[0] 
    
    df = (
        spark.read.schema(stock_schema)
        .option("header", True)
        .csv(csv_path)
    )

    df = (
        df.withColumn("ticker", lit(ticker))
          .withColumn("date", col("date").cast("date"))
          .withColumn("year", year(col("date")))
          .withColumn("month", month(col("date")))
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("ticker", "year", "month")
        .save(output_path)
    )

    print(f"✅ Historical data for {ticker} saved to {output_path}")

def extract_historical_flow(
    csv_dir: str = "data/raw/",
    output_path: str = "s3a://stock-data/raw/"
):
    spark = init_spark("StockExtract")

    for csv_path in glob.glob(os.path.join(csv_dir, "*_history.csv")):
        load_historical_csv(spark, csv_path, output_path)

    spark.stop()

if __name__ == "__main__":
    extract_historical_flow()
