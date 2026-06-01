import os
import duckdb
from utils.logger import get_logger
from utils.config import PROCESSED_DATA_PATH

logger = get_logger("DuckDBAnalytics")

def query_processed_parquet() -> None:
    """
    Demonstrates using DuckDB to query local processed Parquet files.
    Calculates summary metrics directly from Parquet files without querying Postgres.
    """
    logger.info("Initializing DuckDB connection and querying processed Parquet files...")
    
    # Path pattern matching all processed Parquet files
    parquet_pattern = os.path.join(PROCESSED_DATA_PATH, "*.parquet")
    
    # Check if files exist
    if not os.path.exists(PROCESSED_DATA_PATH) or not os.listdir(PROCESSED_DATA_PATH):
        logger.warning("No processed Parquet files found. Run the ETL pipeline first.")
        return
        
    con = duckdb.connect(database=":memory:")
    
    # Query 1: Basic summary metrics for each stock
    query_summary = f"""
        SELECT 
            ticker, 
            COUNT(*) as total_days,
            MIN(date) as start_date,
            MAX(date) as end_date,
            ROUND(AVG(close), 2) as avg_close,
            ROUND(MAX(close), 2) as max_close,
            ROUND(MIN(close), 2) as min_close
        FROM read_parquet('{parquet_pattern}')
        GROUP BY ticker
        ORDER BY ticker;
    """
    
    print("\n" + "=" * 80)
    print(" DUCKDB ANALYTICS: STOCK HISTORICAL SUMMARY (DIRECT FROM PARQUET)")
    print("=" * 80)
    print(con.execute(query_summary).df().to_string(index=False))
    
    # Query 2: Overbought/Oversold alerts based on computed RSI indicator
    query_rsi_alerts = f"""
        SELECT 
            date, 
            ticker, 
            ROUND(close, 2) as close_price, 
            ROUND(rsi, 2) as rsi_value,
            CASE 
                WHEN rsi > 70 THEN 'OVERBOUGHT'
                WHEN rsi < 30 THEN 'OVERSOLD'
            END as status
        FROM read_parquet('{parquet_pattern}')
        WHERE rsi > 70 OR rsi < 30
        ORDER BY date DESC, ticker
        LIMIT 10;
    """
    
    print("\n" + "=" * 80)
    print(" DUCKDB ANALYTICS: RECENT RSI OVERBOUGHT/OVERSOLD ALERTS (LIMIT 10)")
    print("=" * 80)
    print(con.execute(query_rsi_alerts).df().to_string(index=False))
    
    con.close()

if __name__ == "__main__":
    query_processed_parquet()
