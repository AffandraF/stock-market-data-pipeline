import os
import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from utils.spark_builder import init_spark

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def extract_raw(spark, raw_path: str, last_date=None, context_size=20):
    # Read raw data from Delta
    df = spark.read.format("delta").option("mergeSchema", "true").load(raw_path)

    if last_date is None:
        logger.info("Extracted full raw data")
        return df
    else:
        # Include history window before last_date
        window_desc = Window.partitionBy("ticker").orderBy(F.col("date").desc())

        history_ctx = (
            df.join(last_date, "ticker", "inner")
              .filter(F.col("date") <= F.col("last_date"))
              .withColumn("rn", F.row_number().over(window_desc))
              .filter(F.col("rn") <= context_size)
              .drop("rn", "last_date")
        )

        # Include new data after last_date
        new_data = (
            df.join(last_date, "ticker", "left")
              .filter((F.col("last_date").isNull()) | (F.col("date") > F.col("last_date")))
              .drop("last_date")
        )

        df = history_ctx.unionByName(new_data)
        df = df.dropDuplicates(["ticker", "date"])
        logger.info(f"Extracted incremental data: {context_size} rows before + all rows after last_date")        

    return df

def get_last_date(df):
    # Get the last available date for each ticker
    return df.groupBy("ticker").agg(F.max("date").alias("last_date"))

def transform_data(df, last_date=None):
    # Apply technical indicator calculations
    window_spec = Window.partitionBy("ticker").orderBy("date")

    logger.info("Starting technical indicators calculation...")

    # Simple Moving Average
    df = df.withColumn("sma_5", F.avg("close").over(window_spec.rowsBetween(-4, 0)))
    df = df.withColumn("sma_20", F.avg("close").over(window_spec.rowsBetween(-19, 0)))

    # Exponential Moving Average (simplified)
    alpha_12 = 2 / (12 + 1)
    df = df.withColumn(
        "ema_12",
        (1 - alpha_12) * F.lag("close", 1).over(window_spec) + alpha_12 * F.col("close")
    )

    # RSI (14)
    df = df.withColumn("prev_close", F.lag("close", 1).over(window_spec))
    df = df.withColumn("change", F.col("close") - F.col("prev_close"))
    df = df.withColumn("gain", F.when(F.col("change") > 0, F.col("change")).otherwise(0))
    df = df.withColumn("loss", F.when(F.col("change") < 0, -F.col("change")).otherwise(0))
    df = df.withColumn("avg_gain", F.avg("gain").over(window_spec.rowsBetween(-13, 0)))
    df = df.withColumn("avg_loss", F.avg("loss").over(window_spec.rowsBetween(-13, 0)))
    df = df.withColumn("rs", F.when(F.col("avg_loss") == 0, None)
                                 .otherwise(F.col("avg_gain") / F.col("avg_loss")))
    df = df.withColumn("rsi_14", F.when(F.col("rs").isNotNull(),
                                        100 - (100 / (1 + F.col("rs")))).otherwise(100))

    # Bollinger Bands (20)
    df = df.withColumn("rolling_mean", F.avg("close").over(window_spec.rowsBetween(-19, 0)))
    df = df.withColumn("rolling_std", F.stddev("close").over(window_spec.rowsBetween(-19, 0)))
    df = df.withColumn("bollinger_upper", F.col("rolling_mean") + 2 * F.col("rolling_std"))
    df = df.withColumn("bollinger_lower", F.col("rolling_mean") - 2 * F.col("rolling_std"))

    logger.info("Technical indicators calculation completed.")

    # Filter only new rows if incremental
    if last_date is not None:
        df = df.join(last_date, "ticker", "left")
        df = df.filter((F.col("last_date").isNull()) | (F.col("date") > F.col("last_date")))
        df = df.drop("last_date")
        logger.info("Filtered to only new data after last_date")

    return df

def load_processed(df, output_path):
    # Write processed data to Delta Lake
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .save(output_path)
    )
    logger.info(f"Processed Delta table saved at {output_path}")

def load_to_postgres(df):
    # Write final table to PostgreSQL
    table_name = os.getenv("POSTGRES_TABLE", "public.stock_data")
    url = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/stockdb")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    (
        df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", table_name)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )
    logger.info(f"Loaded all data to PostgreSQL table: {table_name}")

def transform_load_flow(
    raw_path: str = "s3a://stock-data-lake/raw/",
    processed_path: str = "s3a://stock-data-lake/processed/"
):
    # Main transform and load pipeline
    spark = init_spark("StockTransform")
    try:
        # Step 1: Extract processed data if exists
        try:
            df_processed = spark.read.format("delta").load(processed_path)
            last_date = get_last_date(df_processed)
        except AnalysisException:
            last_date = None

        # Step 2: Extract raw (full or incremental)
        df_raw = extract_raw(spark, raw_path, last_date, context_size=20)

        # Step 3: Transform
        df_transformed = transform_data(df_raw, last_date)

        logger.info(f"Total transformed rows: {df_transformed.count()}")
        df_transformed.groupBy("ticker").count().show()
        
        # Step 4: Load to Delta Lake
        load_processed(df_transformed, processed_path)

        # Step 5: Load to PostgreSQL
        load_to_postgres(df_transformed)

        logger.info("Transform and load flow completed successfully")

    except Exception as e:
        logger.error(f"Error in transform and load flow: {e}")
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    transform_load_flow()