import os
import duckdb
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

from utils.logger import get_logger
from utils.config import PROCESSED_DATA_PATH

logger = get_logger("DuckDBAnalytics")

def show_summary(con, parquet_pattern: str) -> None:
    # Query 1: Basic summary metrics for each stock
    logger.info("Computing historical price summaries...")
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

    print("\n" + "=" * 85)
    print(" DUCKDB ANALYTICS: STOCK HISTORICAL SUMMARY (DIRECT FROM PARQUET)")
    print("=" * 85)
    print(con.execute(query_summary).df().to_string(index=False))


def show_total_returns(con, parquet_pattern: str) -> None:
    # Query 2: Calculate lifetime returns
    logger.info("Computing lifetime stock returns...")
    query_returns = f"""
        WITH first_last AS (
            SELECT 
                ticker,
                date,
                close,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date ASC) as rn_first,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) as rn_last
            FROM read_parquet('{parquet_pattern}')
        ),
        first_prices AS (
            SELECT ticker, date as start_date, close as start_price
            FROM first_last WHERE rn_first = 1
        ),
        last_prices AS (
            SELECT ticker, date as end_date, close as end_price
            FROM first_last WHERE rn_last = 1
        )
        SELECT 
            f.ticker,
            f.start_date,
            l.end_date,
            ROUND(f.start_price, 2) as initial_price,
            ROUND(l.end_price, 2) as current_price,
            ROUND(((l.end_price - f.start_price) / f.start_price) * 100, 2) as total_return_pct
        FROM first_prices f
        JOIN last_prices l ON f.ticker = l.ticker
        ORDER BY total_return_pct DESC;
    """

    print("\n" + "=" * 85)
    print(" DUCKDB ANALYTICS: LIFETIME INVESTMENT RETURNS")
    print("=" * 85)
    print(con.execute(query_returns).df().to_string(index=False))


def show_rsi_alerts(con, parquet_pattern: str, limit: int = 10) -> None:
    # Query 3: Overbought/Oversold alerts based on computed RSI indicator
    logger.info("Searching for recent RSI alerts...")
    query_rsi_alerts = f"""
        SELECT 
            date, 
            ticker, 
            ROUND(close, 2) as close_price, 
            ROUND(rsi, 2) as rsi_value,
            CASE 
                WHEN rsi > 70 THEN 'OVERBOUGHT (RSI > 70)'
                WHEN rsi < 30 THEN 'OVERSOLD (RSI < 30)'
            END as status
        FROM read_parquet('{parquet_pattern}')
        WHERE rsi > 70 OR rsi < 30
        ORDER BY date DESC, ticker
        LIMIT ?;
    """

    print("\n" + "=" * 85)
    print(f" DUCKDB ANALYTICS: RECENT RSI OVERBOUGHT/OVERSOLD ALERTS (LIMIT {limit})")
    print("=" * 85)
    print(con.execute(query_rsi_alerts, [limit]).df().to_string(index=False))


def show_ma_crossovers(con, parquet_pattern: str, limit: int = 10) -> None:
    # Query 4: 20-day vs 50-day SMA Crossover Detection (Golden / Death Cross)
    logger.info("Detecting recent SMA 20 / SMA 50 crossovers...")
    query_crossovers = f"""
        WITH ranked_data AS (
            SELECT 
                date,
                ticker,
                close,
                sma_20,
                sma_50,
                LAG(sma_20) OVER (PARTITION BY ticker ORDER BY date) as prev_sma_20,
                LAG(sma_50) OVER (PARTITION BY ticker ORDER BY date) as prev_sma_50
            FROM read_parquet('{parquet_pattern}')
        )
        SELECT 
            date,
            ticker,
            ROUND(close, 2) as close_price,
            ROUND(sma_20, 2) as sma_20,
            ROUND(sma_50, 2) as sma_50,
            CASE 
                WHEN prev_sma_20 <= prev_sma_50 AND sma_20 > sma_50 THEN 'GOLDEN CROSS (BULLISH)'
                WHEN prev_sma_20 >= prev_sma_50 AND sma_20 < sma_50 THEN 'DEATH CROSS (BEARISH)'
            END as crossover_type
        FROM ranked_data
        WHERE (prev_sma_20 <= prev_sma_50 AND sma_20 > sma_50) OR (prev_sma_20 >= prev_sma_50 AND sma_20 < sma_50)
        ORDER BY date DESC
        LIMIT ?;
    """

    print("\n" + "=" * 85)
    print(f" DUCKDB ANALYTICS: RECENT SMA crossovers (LIMIT {limit})")
    print("=" * 85)
    print(con.execute(query_crossovers, [limit]).df().to_string(index=False))


def show_volume_spikes(con, parquet_pattern: str, limit: int = 10) -> None:
    # Query 5: Volume Spike Detection (>2.5x of the last 20 days average volume)
    logger.info("Detecting volume spike anomalies...")
    query_spikes = f"""
        WITH vol_stats AS (
            SELECT 
                date,
                ticker,
                volume,
                close,
                AVG(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) as avg_vol_20d
            FROM read_parquet('{parquet_pattern}')
        )
        SELECT 
            date,
            ticker,
            volume,
            ROUND(avg_vol_20d, 0) as avg_volume_20d,
            ROUND(volume / NULLIF(avg_vol_20d, 0), 2) as spike_ratio,
            ROUND(close, 2) as close_price
        FROM vol_stats
        WHERE volume > 2.5 * avg_vol_20d AND avg_vol_20d > 0
        ORDER BY date DESC, spike_ratio DESC
        LIMIT ?;
    """

    print("\n" + "=" * 85)
    print(f" DUCKDB ANALYTICS: RECENT VOLUME SPIKES > 2.5x (LIMIT {limit})")
    print("=" * 85)
    print(con.execute(query_spikes, [limit]).df().to_string(index=False))


def query_processed_parquet() -> None:
    parquet_pattern = os.path.join(PROCESSED_DATA_PATH, "*.parquet")
    
    # Check if files exist
    if not os.path.exists(PROCESSED_DATA_PATH) or not os.listdir(PROCESSED_DATA_PATH):
        logger.warning(f"No processed Parquet files found in {PROCESSED_DATA_PATH}. Run the ETL pipeline first.")
        return
        
    con = duckdb.connect(database=":memory:")
    
    logger.info("Initializing DuckDB query engine...")
    
    try:
        show_summary(con, parquet_pattern)
        show_total_returns(con, parquet_pattern)
        show_rsi_alerts(con, parquet_pattern, limit=10)
        show_ma_crossovers(con, parquet_pattern, limit=10)
        show_volume_spikes(con, parquet_pattern, limit=10)
    except Exception as e:
        logger.error(f"Error executing analytics: {e}")
    finally:
        con.close()


if __name__ == "__main__":
    query_processed_parquet()
