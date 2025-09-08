# src/transform.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from utils.spark_builder import init_spark
import psycopg2
import os

def extract_raw(spark, raw_path: str):
    df = spark.read.format("delta").load(raw_path)
    print(f"✅ Extract raw data from {raw_path}")
    return df

def transform_data(df):
    windowSpec = Window.partitionBy("ticker").orderBy("date")

    print("Starting technical indicators calculation...")

    # SMA
    print("Calculating SMA (5 & 20)")
    df = df.withColumn("SMA_5", F.avg("close").over(windowSpec.rowsBetween(-4, 0)))
    df = df.withColumn("SMA_20", F.avg("close").over(windowSpec.rowsBetween(-19, 0)))

    # EMA (approximation with lag)
    print("Calculating EMA (12)")
    alpha_12 = 2 / (12 + 1)
    df = df.withColumn(
        "EMA_12",
        (1 - alpha_12) * F.lag("close", 1).over(windowSpec) + alpha_12 * F.col("close")
    )

    # RSI
    print("Calculating RSI (14)")
    df = df.withColumn("prev_close", F.lag("close", 1).over(windowSpec))
    df = df.withColumn("change", F.col("close") - F.col("prev_close"))
    df = df.withColumn("gain", F.when(F.col("change") > 0, F.col("change")).otherwise(0))
    df = df.withColumn("loss", F.when(F.col("change") < 0, -F.col("change")).otherwise(0))

    df = df.withColumn("avg_gain", F.avg("gain").over(windowSpec.rowsBetween(-13, 0)))
    df = df.withColumn("avg_loss", F.avg("loss").over(windowSpec.rowsBetween(-13, 0)))

    # RS
    df = df.withColumn(
        "RS",
        F.when(F.col("avg_loss") == 0, None).otherwise(F.col("avg_gain") / F.col("avg_loss"))
    )

    # RSI
    df = df.withColumn(
        "RSI_14",
        F.when(F.col("RS").isNotNull(), 100 - (100 / (1 + F.col("RS")))).otherwise(100)
    )

    # Bollinger Bands
    print("Calculating Bollinger Bands (20)")
    df = df.withColumn("rolling_mean", F.avg("close").over(windowSpec.rowsBetween(-19, 0)))
    df = df.withColumn("rolling_std", F.stddev("close").over(windowSpec.rowsBetween(-19, 0)))
    df = df.withColumn("Bollinger_Upper", F.col("rolling_mean") + 2 * F.col("rolling_std"))
    df = df.withColumn("Bollinger_Lower", F.col("rolling_mean") - 2 * F.col("rolling_std"))

    print("Technical indicators calculation completed.")
    return df

def load_processed(df, output_path, last_date=None):
    if last_date is None:
        # Full load
        df.write.format("delta") \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .save(output_path)
        print(f"✅ Delta table created (overwrite) at {output_path}")
    else:
        df_to_append = df.filter(F.col("date") > F.to_date(F.lit(last_date)))

        if df_to_append.count() > 0:
            df_to_append.write.format("delta") \
                .mode("append") \
                .partitionBy("year", "month") \
                .option("mergeSchema", "true") \
                .save(output_path)
            print(f"✅ Appended {df_to_append.count()} rows to Delta table at {output_path}")
        else:
            print("No new data to append")
        

def load_to_postgres(df, last_date=None):
    table_name = os.getenv("POSTGRES_TABLE", "public.stock_data")
    url = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/stockdb")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    if last_date is None:
        df.write.format("jdbc") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print(f"✅ Load all data to {table_name}")
        return
    else:
        df_to_append = df.filter(F.col("date") > F.to_date(F.lit(last_date)))

        if df_to_append.count() > 0:
            df_to_append.write.format("jdbc") \
                .option("url", url) \
                .option("dbtable", table_name) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print(f"✅ Appended {df_to_append.count()} rows to Postgres table {table_name}")
        else:
            print("No new data to append")

def build_incremental(df_raw, df_processed, context_size=20):
    # Get last processed date per ticker
    last_date = df_processed.groupBy("ticker").agg(F.max("date").alias("last_date"))

    new_data = (
        df_raw.join(last_date, "ticker", "left")
              .filter((F.col("last_date").isNull()) | (F.col("date") > F.col("last_date")))
    )

    windowDesc = Window.partitionBy("ticker").orderBy(F.col("date").desc())

    history_context = (
        df_raw.join(last_date, "ticker", "inner")
              .filter(F.col("date") <= F.col("last_date"))
              .withColumn("rn", F.row_number().over(windowDesc))
              .filter(F.col("rn") <= context_size)
              .drop("rn")
    )

    return history_context.unionByName(new_data), last_date

def transform_load_flow(
    raw_path: str = "s3a://stock-data-lake/raw/",
    processed_path: str = "s3a://stock-data-lake/processed/"
):
    spark = init_spark("StockTransform")
    df_raw = extract_raw(spark, raw_path)

    try:
        df_raw = extract_raw(spark, raw_path)
    
        try:
            df_processed = spark.read.format("delta").load(processed_path)
            df_incremental, last_date = build_incremental(df_raw, df_processed)
            df_transformed = transform_data(df_incremental)
            df_final = (
                df_transformed
                .withColumn("year", F.col("date").substr(1, 4))
                .withColumn("month", F.col("date").substr(6, 2))
            )
            
            load_processed(df_final, processed_path, last_date)
            load_to_postgres(df_final, last_date)        
        except AnalysisException:
            print("No processed found, running full transform...")
            df_transformed = transform_data(df_raw)
            df_final = (
                df_transformed
                .withColumn("year", F.col("date").substr(1, 4))
                .withColumn("month", F.col("date").substr(6, 2))
            )

            load_processed(df_final, processed_path)
            load_to_postgres(df_final)
    finally:
        spark.stop()

if __name__ == "__main__":
    transform_load_flow()