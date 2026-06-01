import os
import pandas as pd
from utils.logger import get_logger
from utils.config import RAW_DATA_PATH, PROCESSED_DATA_PATH, STOCK_TICKERS

logger = get_logger("Transformer")

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates technical indicators (SMA 20/50, EMA 20, RSI 14, MACD, Bollinger Bands) for a stock dataframe.
    """
    df = df.copy()
    
    # Sort by date ascending to calculate rolling indicators correctly
    df = df.sort_values("date").reset_index(drop=True)
    
    # 1. Simple Moving Averages (SMA)
    df["sma_20"] = df["close"].rolling(window=20).mean()
    df["sma_50"] = df["close"].rolling(window=50).mean()
    
    # 2. Exponential Moving Average (EMA)
    df["ema_20"] = df["close"].ewm(span=20, adjust=False).mean()
    
    # 3. Relative Strength Index (RSI 14)
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    
    # Use Welles Wilder's smoothing or standard ewm for RSI
    avg_gain = gain.ewm(alpha=1/14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, adjust=False).mean()
    
    rs = avg_gain / avg_loss
    df["rsi"] = 100.0 - (100.0 / (1.0 + rs))
    
    # Handle edge case where average loss is 0 (RSI = 100)
    df.loc[avg_loss == 0, "rsi"] = 100.0
    
    # 4. MACD (Moving Average Convergence Divergence)
    ema_12 = df["close"].ewm(span=12, adjust=False).mean()
    ema_26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = ema_12 - ema_26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]
    
    # 5. Bollinger Bands (20 days, 2 standard deviations)
    bb_mean = df["close"].rolling(window=20).mean()
    bb_std = df["close"].rolling(window=20).std()
    df["bollinger_upper"] = bb_mean + (2 * bb_std)
    df["bollinger_lower"] = bb_mean - (2 * bb_std)
    
    return df

def transform_stock(ticker: str, raw_dir: str = RAW_DATA_PATH, processed_dir: str = PROCESSED_DATA_PATH) -> None:
    """
    Reads validated raw stock data, standardizes columns, calculates indicators,
    and saves the processed dataset as Parquet.
    """
    logger.info(f"Starting transformation for ticker: {ticker}")
    input_path = os.path.join(raw_dir, f"{ticker}_validated.parquet")
    output_path = os.path.join(processed_dir, f"{ticker}.parquet")
    
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Validated data file not found: {input_path}")
        
    df = pd.read_parquet(input_path)
    
    # Standardize columns to lowercase
    df.columns = df.columns.str.lower()
    
    # Add ticker column
    df["ticker"] = ticker
    
    # Compute technical indicators
    df_transformed = compute_indicators(df)
    
    # Ensure processed directory exists
    os.makedirs(processed_dir, exist_ok=True)
    
    # Save transformed data to processed layer
    df_transformed.to_parquet(output_path, index=False)
    logger.info(f"Successfully processed and saved transformed Parquet to {output_path} ({len(df_transformed)} rows)")

def transform_all() -> None:
    """
    Transforms all tickers configured in STOCK_TICKERS.
    """
    logger.info("Transforming all validated stock files...")
    for ticker in STOCK_TICKERS:
        try:
            transform_stock(ticker)
        except Exception as e:
            logger.error(f"Error transforming {ticker}: {e}")
            raise

if __name__ == "__main__":
    transform_all()
