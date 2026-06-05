import os
import datetime
import pandas as pd
from psycopg2.extras import execute_values
from utils.logger import get_logger
from utils.db import get_db_connection
from utils.config import PROCESSED_DATA_PATH, STOCK_TICKERS

logger = get_logger("Loader")

def get_last_loaded_date(ticker: str, conn) -> datetime.date:
    # Gets latest loaded date for ticker, or None.
    query = "SELECT MAX(date) FROM fact_stock_price WHERE ticker = %s;"
    with conn.cursor() as cur:
        cur.execute(query, (ticker,))
        res = cur.fetchone()
        if res and res[0]:
            return res[0]
    return None

def load_date_dimension(dates: list, conn) -> None:
    # Generates and inserts records into dim_date.
    logger.info(f"Loading {len(dates)} dates into dim_date...")
    
    date_records = []
    for d_str in dates:
        d = pd.to_datetime(d_str).date()
        day = d.day
        month = d.month
        year = d.year
        quarter = (month - 1) // 3 + 1
        day_of_week = d.weekday()  # Monday is 0, Sunday is 6
        is_weekend = day_of_week >= 5
        
        date_records.append((
            d_str,
            day,
            month,
            year,
            quarter,
            int(day_of_week),
            bool(is_weekend)
        ))
        
    query = """
        INSERT INTO dim_date (date, day, month, year, quarter, day_of_week, is_weekend)
        VALUES %s
        ON CONFLICT (date) DO NOTHING;
    """
    with conn.cursor() as cur:
        execute_values(cur, query, date_records)

def load_fact_prices(df: pd.DataFrame, conn) -> None:
    # Loads stock prices into fact_stock_price.
    logger.info(f"Loading {len(df)} rows into fact_stock_price...")
    
    # Prepare records as tuples, converting NaN to None for NULL insertion
    price_records = []
    for _, row in df.iterrows():
        price_records.append((
            str(row["date"]),
            str(row["ticker"]),
            None if pd.isna(row["open"]) else float(row["open"]),
            None if pd.isna(row["high"]) else float(row["high"]),
            None if pd.isna(row["low"]) else float(row["low"]),
            None if pd.isna(row["close"]) else float(row["close"]),
            None if pd.isna(row["volume"]) else int(row["volume"])
        ))
        
    query = """
        INSERT INTO fact_stock_price (date, ticker, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (date, ticker) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume;
    """
    with conn.cursor() as cur:
        execute_values(cur, query, price_records)

def load_fact_indicators(df: pd.DataFrame, conn) -> None:
    # Loads technical indicators into fact_stock_indicator.
    logger.info(f"Loading {len(df)} rows into fact_stock_indicator...")
    
    indicator_records = []
    for _, row in df.iterrows():
        indicator_records.append((
            str(row["date"]),
            str(row["ticker"]),
            None if pd.isna(row["sma_20"]) else float(row["sma_20"]),
            None if pd.isna(row["sma_50"]) else float(row["sma_50"]),
            None if pd.isna(row["ema_20"]) else float(row["ema_20"]),
            None if pd.isna(row["rsi"]) else float(row["rsi"]),
            None if pd.isna(row["macd"]) else float(row["macd"]),
            None if pd.isna(row["macd_signal"]) else float(row["macd_signal"]),
            None if pd.isna(row["macd_hist"]) else float(row["macd_hist"]),
            None if pd.isna(row["bollinger_upper"]) else float(row["bollinger_upper"]),
            None if pd.isna(row["bollinger_lower"]) else float(row["bollinger_lower"])
        ))
        
    query = """
        INSERT INTO fact_stock_indicator (
            date, ticker, sma_20, sma_50, ema_20, rsi, macd, macd_signal, macd_hist, bollinger_upper, bollinger_lower
        )
        VALUES %s
        ON CONFLICT (date, ticker) DO UPDATE SET
            sma_20 = EXCLUDED.sma_20,
            sma_50 = EXCLUDED.sma_50,
            ema_20 = EXCLUDED.ema_20,
            rsi = EXCLUDED.rsi,
            macd = EXCLUDED.macd,
            macd_signal = EXCLUDED.macd_signal,
            macd_hist = EXCLUDED.macd_hist,
            bollinger_upper = EXCLUDED.bollinger_upper,
            bollinger_lower = EXCLUDED.bollinger_lower;
    """
    with conn.cursor() as cur:
        execute_values(cur, query, indicator_records)

def load_stock(ticker: str, processed_dir: str = PROCESSED_DATA_PATH) -> None:
    # Runs incremental or full data load for ticker.
    logger.info(f"Starting load process for ticker: {ticker}")
    input_path = os.path.join(processed_dir, f"{ticker}.parquet")
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Processed file not found for loading: {input_path}")
        
    df = pd.read_parquet(input_path)
    
    # Establish connection
    conn = get_db_connection()
    try:
        # Determine incremental threshold
        last_date = get_last_loaded_date(ticker, conn)
        
        if last_date:
            logger.info(f"Last loaded date for {ticker} in DB is {last_date}")
            # Filter for rows newer than last loaded date
            df["date_parsed"] = pd.to_datetime(df["date"]).dt.date
            df_new = df[df["date_parsed"] > last_date].copy()
            df_new = df_new.drop(columns=["date_parsed"])
        else:
            logger.info(f"No existing data found for {ticker} in DB. Performing full load.")
            df_new = df.copy()
            
        if df_new.empty:
            logger.info(f"No new data to load for {ticker}.")
            return
            
        logger.info(f"Found {len(df_new)} new rows to load for {ticker}.")
        
        # 1. Load dates to Date Dimension
        new_dates = df_new["date"].unique().tolist()
        load_date_dimension(new_dates, conn)
        
        # 2. Load prices to Price Fact Table
        load_fact_prices(df_new, conn)
        
        # 3. Load indicators to Indicator Fact Table
        load_fact_indicators(df_new, conn)
        
        conn.commit()
        logger.info(f"Successfully loaded stock {ticker} to database.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading stock {ticker}: {e}")
        raise
    finally:
        conn.close()

def load_all() -> None:
    # Loads all configured stock tickers.
    logger.info("Loading all processed stock files...")
    for ticker in STOCK_TICKERS:
        try:
            load_stock(ticker)
        except Exception as e:
            logger.error(f"Error loading {ticker}: {e}")
            raise

if __name__ == "__main__":
    load_all()
