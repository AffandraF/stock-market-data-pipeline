import os
import pandas as pd
from utils.logger import get_logger
from utils.config import RAW_DATA_PATH, STOCK_TICKERS

logger = get_logger("Validator")

def validate_stock(ticker: str, raw_dir: str = RAW_DATA_PATH) -> None:
    # Validates schema, nulls, duplicates, and business rules, then saves Parquet.
    logger.info(f"Starting validation for ticker: {ticker}")
    input_path = os.path.join(raw_dir, f"{ticker}.parquet")
    output_path = os.path.join(raw_dir, f"{ticker}_validated.parquet")
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Raw data file not found for validation: {input_path}")
        
    df = pd.read_parquet(input_path)
    initial_rows = len(df)
    
    # 1. Schema Validation (Column Presence)
    expected_cols = ["Date", "Open", "High", "Low", "Close", "Volume"]
    missing_cols = [c for c in expected_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Schema Validation Failed for {ticker}. Missing columns: {missing_cols}")
        
    # Ensure only expected columns are kept
    df = df[expected_cols].copy()
    
    # 2. Schema Validation (Column Types)
    try:
        df["Date"] = df["Date"].astype(str)
        df["Open"] = pd.to_numeric(df["Open"], errors="coerce")
        df["High"] = pd.to_numeric(df["High"], errors="coerce")
        df["Low"] = pd.to_numeric(df["Low"], errors="coerce")
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce").astype("Int64")  # Null-friendly integer
    except Exception as e:
        raise TypeError(f"Schema Type Conversion Failed for {ticker}: {e}")
        
    # 3. Duplicate Validation
    # Identify duplicate dates
    duplicate_count = df.duplicated(subset=["Date"]).sum()
    if duplicate_count > 0:
        logger.warning(f"Found {duplicate_count} duplicate Date rows for {ticker}. Dropping duplicates.")
        df = df.drop_duplicates(subset=["Date"])
        
    # 4. Null Value Validation
    # Identify rows with null values in required columns
    required_cols = ["Date", "Open", "High", "Low", "Close", "Volume"]
    null_mask = df[required_cols].isnull().any(axis=1)
    null_count = null_mask.sum()
    if null_count > 0:
        logger.warning(f"Found {null_count} rows with nulls in required columns for {ticker}. Dropping these rows.")
        df = df[~null_mask]
        
    # 5. Business Rules Validation
    # Close price > 0
    invalid_price_mask = df["Close"] <= 0
    invalid_price_count = invalid_price_mask.sum()
    if invalid_price_count > 0:
        logger.warning(f"Found {invalid_price_count} rows with close price <= 0 for {ticker}. Dropping these rows.")
        df = df[~invalid_price_mask]
        
    # Volume >= 0
    invalid_volume_mask = df["Volume"] < 0
    invalid_volume_count = invalid_volume_mask.sum()
    if invalid_volume_count > 0:
        logger.warning(f"Found {invalid_volume_count} rows with negative volume for {ticker}. Dropping these rows.")
        df = df[~invalid_volume_mask]
        
    # Validation summary
    final_rows = len(df)
    logger.info(f"Validation completed for {ticker}: initial rows={initial_rows}, final rows={final_rows}, dropped rows={initial_rows - final_rows}")
    
    # Write to validated parquet path
    df.to_parquet(output_path, index=False)
    logger.info(f"Validated dataset saved to {output_path}")

def validate_all() -> None:
    # Validates all configured stock tickers.
    logger.info("Validating all raw stock files...")
    for ticker in STOCK_TICKERS:
        try:
            validate_stock(ticker)
        except Exception as e:
            logger.error(f"Error validating {ticker}: {e}")
            raise

if __name__ == "__main__":
    validate_all()
