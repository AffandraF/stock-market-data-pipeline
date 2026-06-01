import os
import pandas as pd
import yfinance as yf
from utils.logger import get_logger
from utils.config import RAW_DATA_PATH, STOCK_TICKERS, USE_LOCAL_DATA

logger = get_logger("Extractor")

def extract_stock(ticker: str, raw_dir: str = RAW_DATA_PATH) -> None:
    """
    Extracts stock data for a given ticker from Yahoo Finance or CSV fallback,
    and saves it as a raw Parquet file.
    """
    logger.info(f"Starting extraction for ticker: {ticker}")
    df = None
    
    # Try downloading from yfinance first unless offline mode is explicitly requested
    if not USE_LOCAL_DATA:
        ticker_jk = f"{ticker}.JK"
        try:
            logger.info(f"Attempting to download data from Yahoo Finance for {ticker_jk}...")
            # Download recent history (e.g., past 5 years to cover all historical data plus incremental)
            ticker_data = yf.Ticker(ticker_jk)
            df_downloaded = ticker_data.history(period="5y")
            
            if not df_downloaded.empty:
                df = df_downloaded.reset_index()
                # Standardize columns to match CSV layout
                df = df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
                # Convert date to string format YYYY-MM-DD
                df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")
                logger.info(f"Successfully downloaded {len(df)} rows from yfinance for {ticker}")
            else:
                logger.warning(f"Yahoo Finance returned empty data for {ticker_jk}")
                
        except Exception as e:
            logger.warning(f"Failed to fetch data from yfinance for {ticker}: {e}. Proceeding with CSV fallback.")
    else:
        logger.info("USE_LOCAL_DATA is set to True. Skipping yfinance download and forcing CSV dummy data usage.")


    # Fallback to local CSV files if yfinance failed or returned empty
    if df is None or df.empty:
        logger.info(f"Using CSV fallback files for {ticker}...")
        history_path = os.path.join(raw_dir, f"{ticker}_history.csv")
        kafka_path = os.path.join(raw_dir, f"{ticker}_kafka.csv")
        
        dfs = []
        if os.path.exists(history_path):
            df_hist = pd.read_csv(history_path)
            dfs.append(df_hist)
            logger.info(f"Loaded historical CSV: {history_path} ({len(df_hist)} rows)")
            
        if os.path.exists(kafka_path):
            df_kafka = pd.read_csv(kafka_path)
            dfs.append(df_kafka)
            logger.info(f"Loaded incremental CSV: {kafka_path} ({len(df_kafka)} rows)")
            
        if dfs:
            df = pd.concat(dfs, ignore_index=True)
            df = df.drop_duplicates(subset=["Date"])
            # Sort by Date ascending
            df = df.sort_values("Date").reset_index(drop=True)
        else:
            raise FileNotFoundError(f"No source data found for ticker {ticker} (yfinance failed, and no CSV files exist)")

    # Save raw data as Parquet file
    os.makedirs(raw_dir, exist_ok=True)
    output_path = os.path.join(raw_dir, f"{ticker}.parquet")
    df.to_parquet(output_path, index=False)
    logger.info(f"Successfully saved raw parquet to {output_path} ({len(df)} rows)")

def extract_all() -> None:
    """
    Extracts all tickers configured in STOCK_TICKERS.
    """
    logger.info("Extracting all stock tickers...")
    for ticker in STOCK_TICKERS:
        try:
            extract_stock(ticker)
        except Exception as e:
            logger.error(f"Error extracting {ticker}: {e}")
            raise

if __name__ == "__main__":
    extract_all()
