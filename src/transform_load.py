# src/transform.py
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from utils.spark_builder import init_spark
import os

def extract_raw(spark, raw_path: str, last_date=None, context_size=20):
    df = spark.read.format("delta").load(raw_path)

    if last_date is not None:
        # History context: 20 rows up to last_date
        windowDesc = Window.partitionBy("ticker").orderBy(F.col("date").desc())
        history_context = (
            df.join(last_date, "ticker", "inner")
              .filter(F.col("date") <= F.col("last_date"))
              .withColumn("rn", F.row_number().over(windowDesc))
              .filter(F.col("rn") <= context_size)          
              .drop("rn", "last_date")
        )

        # New data after last_date
        new_data = (
            df.join(last_date, "ticker", "left")
              .filter((F.col("last_date").isNull()) | (F.col("date") > F.col("last_date")))
              .drop("last_date")
        )

        df = history_context.unionByName(new_data)
        print(f"Extracted incremental data: {context_size} rows before + all rows after last_date")
    else:
        print("Extracted full raw data")

    return df

def get_last_date(df):
    return df.groupBy("ticker").agg(F.max("date").alias("last_date"))


def transform_data(df, last_date=None):
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

    if last_date is None:
        return df
    else:
        df = df.join(last_date, "ticker", "left")
        df = df.filter((F.col("last_date").isNull()) | (F.col("date") > F.col("last_date")))
        df = df.drop("last_date")
        print("Filtered to only new data after last_date")
        return df

def load_processed(df, output_path):
    df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .save(output_path)
    print(f"Delta table created (overwrite) at {output_path}")
        
def load_to_postgres(df):
    table_name = os.getenv("POSTGRES_TABLE", "public.stock_data")
    url = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/stockdb")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    df.write.format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    print(f"Load all data to {table_name}")
    return

def transform_load_flow(
    raw_path: str = "s3a://stock-data-lake/raw/",
    processed_path: str = "s3a://stock-data-lake/processed/"
):
    spark = init_spark("StockTransform")

    try:
        # Step 1: Extract processed
        try:
            df_processed = spark.read.format("delta").load(processed_path)
            last_date = df_processed.groupBy("ticker").agg(F.max("date").alias("last_date"))
        except AnalysisException:
            last_date = None

        # Step 2: Extract raw (full or incremental depends on last_date)
        df_raw = extract_raw(spark, raw_path, last_date, context_size=20)

        # Step 3: Transform
        df_transformed = transform_data(df_raw, last_date)

        # Step 4: Load to Delta Lake
        load_processed(df_transformed, processed_path, last_date)
        # Step 5: Load to Postgres
        load_to_postgres(df_transformed, last_date)
    finally:
        spark.stop()

if __name__ == "__main__":
    transform_load_flow()