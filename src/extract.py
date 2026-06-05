import os
import pandas as pd
import yfinance as yf
from utils.logger import get_logger
from utils.config import RAW_DATA_PATH, STOCK_TICKERS, EXTRACTION_MODE

logger = get_logger("Extractor")

def extract_stock(ticker: str, raw_dir: str = RAW_DATA_PATH) -> None:
    # Extracts stock data from API or CSV and saves as raw Parquet.
    mode = EXTRACTION_MODE.lower()
    if mode not in ("api", "csv", "hybrid"):
        logger.warning(f"Invalid extraction mode '{mode}' configured. Defaulting to 'hybrid'.")
        mode = "hybrid"

    logger.info(f"Starting extraction for ticker: {ticker} (Mode: {mode.upper()})")
    df = None
    
    # 1. API Fetching (for 'api' or 'hybrid' modes)
    if mode in ("api", "hybrid"):
        ticker_jk = f"{ticker}.JK"
        try:
            logger.info(f"Attempting to download data from Yahoo Finance for {ticker_jk}...")
            # Download recent history (past 5 years)
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
                if mode == "api":
                    raise ValueError(f"Yahoo Finance returned empty data for {ticker_jk} in API-only mode.")
                
        except Exception as e:
            logger.warning(f"Failed to fetch data from yfinance for {ticker}: {e}.")
            if mode == "api":
                raise e

    # 2. Local CSV Fetching (for 'csv' mode, or fallback in 'hybrid' mode)
    if df is None or df.empty:
        if mode == "api":
            raise ValueError(f"Extraction failed for ticker {ticker}: API returned no data and fallback is disabled.")
            
        logger.info(f"Using CSV fallback/source files for {ticker}...")
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
            raise FileNotFoundError(f"No source data found for ticker {ticker} (no CSV files exist)")

    # Save raw data as Parquet file
    os.makedirs(raw_dir, exist_ok=True)
    output_path = os.path.join(raw_dir, f"{ticker}.parquet")
    df.to_parquet(output_path, index=False)
    logger.info(f"Successfully saved raw parquet to {output_path} ({len(df)} rows)")

def extract_all() -> None:
    # Extracts all configured stock tickers.
    logger.info(f"Extracting all stock tickers (Mode: {EXTRACTION_MODE.upper()})...")
    for ticker in STOCK_TICKERS:
        try:
            extract_stock(ticker)
        except Exception as e:
            logger.error(f"Error extracting {ticker}: {e}")
            raise

if __name__ == "__main__":
    extract_all()
