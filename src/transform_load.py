# src/transform.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from prefect import task, flow, get_run_logger
from utils.spark_builder import init_spark

def transform_data(df):
    """Step 3: Transform with technical indicators."""
    windowSpec = Window.partitionBy("ticker").orderBy("date") if "ticker" in df.columns else Window.orderBy("date")

    print("📊 Starting technical indicators calculation...")

    # SMA
    print("➡️ Calculating SMA (5 & 20)")
    df = df.withColumn("SMA_5", F.avg("close").over(windowSpec.rowsBetween(-4, 0)))
    df = df.withColumn("SMA_20", F.avg("close").over(windowSpec.rowsBetween(-19, 0)))

    # EMA (approximation with lag)
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
    print("➡️ Calculating Bollinger Bands (20)")
    df = df.withColumn("rolling_mean", F.avg("close").over(windowSpec.rowsBetween(-19, 0)))
    df = df.withColumn("rolling_std", F.stddev("close").over(windowSpec.rowsBetween(-19, 0)))
    df = df.withColumn("Bollinger_Upper", F.col("rolling_mean") + 2 * F.col("rolling_std"))
    df = df.withColumn("Bollinger_Lower", F.col("rolling_mean") - 2 * F.col("rolling_std"))

    print("✅ Technical indicators calculation completed.")
    return df

@task
def read_silver_layer(spark, silver_path: str):
    logger = get_run_logger()
    df = spark.read.format("delta").load(silver_path)
    logger.info(f"✅ Loaded Silver Layer data from {silver_path}")
    return df

@task
def load_processed(df, output_path: str):
    logger = get_run_logger()
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy("ticker", "year", "month")   # ✅ tambah ticker di partition
        .save(output_path)
    )
    logger.info(f"✅ Saved Gold Layer data (partitioned) to {output_path}")

@task
def load_to_postgres(df):
    logger = get_run_logger()
    (
        df.write.format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/stockdb")
        .option("dbtable", "public.stock_gold")
        .option("user", "your_user")
        .option("password", "your_password")
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )
    logger.info("✅ Saved Gold Layer data to Postgres")

@flow(name="daily-gold-transform")
def transform_load_flow(
    #TODO: fix paths name
    silver_path = "s3a://stock-data/raw/",
    gold_path = "s3a://stock-data/processed/"
):
    spark = init_spark("StockTransform")

    df_silver = read_silver_layer(spark, silver_path)
    df_transformed = transform_data(df_silver)

    # Extract year and month for partitioning
    df_final = (
        df_transformed
        .withColumn("year", df_transformed["date"].substr(1, 4))
        .withColumn("month", df_transformed["date"].substr(6, 2))
    )

    load_processed(df_final, gold_path)
    load_to_postgres(df_final)
    spark.stop()