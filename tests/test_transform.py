import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

from src.validate import validate_stock
from src.transform import compute_indicators

def test_compute_indicators() -> None:
    # Tests technical indicator calculation on dummy data.
    # Create 60 days of mock stock data
    dates = pd.date_range(start="2026-01-01", periods=60).strftime("%Y-%m-%d")
    close_prices = [100.0 + i for i in range(60)]  # Steadily increasing close price
    
    mock_df = pd.DataFrame({
        "date": dates,
        "open": [99.0 + i for i in range(60)],
        "high": [101.0 + i for i in range(60)],
        "low": [98.0 + i for i in range(60)],
        "close": close_prices,
        "volume": [10000 + i * 100 for i in range(60)],
        "ticker": ["TEST"] * 60
    })
    
    transformed_df = compute_indicators(mock_df)
    
    # 1. Check SMA 20 columns exist
    assert "sma_20" in transformed_df.columns
    # Check that first 19 rows are NaN for SMA 20
    assert transformed_df["sma_20"].iloc[0:19].isna().all()
    # Check 20th row is the mean of first 20 close prices
    expected_sma_20 = np.mean(close_prices[0:20])
    assert pytest.approx(transformed_df["sma_20"].iloc[19]) == expected_sma_20
    
    # 2. Check SMA 50 columns exist
    assert "sma_50" in transformed_df.columns
    assert transformed_df["sma_50"].iloc[0:49].isna().all()
    expected_sma_50 = np.mean(close_prices[0:50])
    assert pytest.approx(transformed_df["sma_50"].iloc[49]) == expected_sma_50
    
    # 3. Check EMA 20, RSI, MACD, and Bollinger Bands columns exist
    assert "ema_20" in transformed_df.columns
    assert "rsi" in transformed_df.columns
    assert "macd" in transformed_df.columns
    assert "macd_signal" in transformed_df.columns
    assert "macd_hist" in transformed_df.columns
    assert "bollinger_upper" in transformed_df.columns
    assert "bollinger_lower" in transformed_df.columns

def test_validator_logic(tmp_path) -> None:
    # Tests validator logic using temporary Parquet files.
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    
    # Mock invalid data:
    # Row 1: Valid
    # Row 2: Duplicate date
    # Row 3: Null in Open
    # Row 4: Close price <= 0
    # Row 5: Volume < 0
    mock_data = pd.DataFrame({
        "Date": ["2026-01-01", "2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"],
        "Open": [100.0, 100.0, None, 105.0, 110.0],
        "High": [102.0, 102.0, 101.0, 106.0, 112.0],
        "Low": [98.0, 98.0, 99.0, 104.0, 108.0],
        "Close": [101.0, 101.0, 100.0, -1.0, 111.0],
        "Volume": [10000, 10000, 20000, 15000, -500]
    })
    
    ticker = "MOCK"
    raw_file = raw_dir / f"{ticker}.parquet"
    mock_data.to_parquet(raw_file, index=False)
    
    # Run validation
    validate_stock(ticker, raw_dir=str(raw_dir))
    
    validated_file = raw_dir / f"{ticker}_validated.parquet"
    assert validated_file.exists()
    
    validated_df = pd.read_parquet(validated_file)
    
    # Expected results:
    # - Duplicates removed (Row 2 removed)
    # - Null in required fields removed (Row 3 removed)
    # - Close price <= 0 removed (Row 4 removed)
    # - Volume < 0 removed (Row 5 removed)
    # Only Row 1 should survive validation!
    assert len(validated_df) == 1
    assert validated_df["Date"].iloc[0] == "2026-01-01"
    assert validated_df["Close"].iloc[0] == 101.0
