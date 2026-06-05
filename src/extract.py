import os
import pandas as pd
import yfinance as yf
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

from utils.logger import get_logger
from utils.config import RAW_DATA_PATH, STOCK_TICKERS

# Extraction mode configuration:
# - 'api': Fetch only from Yahoo Finance API.
# - 'csv': Fetch only from local CSV files.
EXTRACTION_MODE = "csv"

logger = get_logger("Extractor")

def extract_stock(ticker: str, raw_dir: Path = RAW_DATA_PATH) -> None:
    # Extracts stock data from API or CSV and saves as raw Parquet.
    mode = EXTRACTION_MODE.lower()
    if mode not in ("api", "csv"):
        logger.warning(f"Invalid extraction mode '{mode}' configured. Defaulting to 'api'.")
        mode = "api"

    logger.info(f"Starting extraction for ticker: {ticker} (Mode: {mode.upper()})")
    df = None
    
    # 1. API Fetching (for 'api' mode)
    if mode == "api":
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
                raise ValueError(f"Yahoo Finance returned empty data for {ticker_jk}")
                
        except Exception as e:
            logger.error(f"Failed to fetch data from yfinance for {ticker}: {e}.")
            raise e

    # 2. Local CSV Fetching (for 'csv' mode)
    elif mode == "csv":
        logger.info(f"Using CSV source files for {ticker}...")
        history_path = os.path.join(raw_dir, f"{ticker}_history.csv")
        
        if os.path.exists(history_path):
            df = pd.read_csv(history_path)
            logger.info(f"Loaded historical CSV: {history_path} ({len(df)} rows)")
            df = df.drop_duplicates(subset=["Date"])
            # Sort by Date ascending
            df = df.sort_values("Date").reset_index(drop=True)
        else:
            raise FileNotFoundError(f"No source data found for ticker {ticker}: {history_path} does not exist")

    # Save raw data as Parquet file
    if df is not None and not df.empty:
        os.makedirs(raw_dir, exist_ok=True)
        output_path = os.path.join(raw_dir, f"{ticker}.parquet")
        df.to_parquet(output_path, index=False)
        logger.info(f"Successfully saved raw parquet to {output_path} ({len(df)} rows)")
    else:
        raise ValueError(f"No data extracted for ticker {ticker}.")

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
