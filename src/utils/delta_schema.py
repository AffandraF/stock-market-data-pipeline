from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def stock_schema():
    return StructType([
        StructField("Date", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", LongType(), True),
    ])