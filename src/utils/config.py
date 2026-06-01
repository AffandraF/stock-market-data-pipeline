import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "stockdb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

# Data Storage Paths
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", "data/raw")
PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_PATH", "data/processed")

# Stock Tickers to process
STOCK_TICKERS = [
    t.strip() for t in os.getenv(
        "STOCK_TICKERS",
        "ADRO,ANTM,ASII,BBCA,BBNI,BMRI,BRIS,PGAS,TLKM,UNTR"
    ).split(",") if t.strip()
]

# Mode selection: Set to True to skip external yfinance API and use local dummy/historical CSVs directly
USE_LOCAL_DATA = os.getenv("USE_LOCAL_DATA", "False").lower() in ("true", "1", "t")

