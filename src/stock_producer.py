import os
from bson import json_util
import yfinance as yf
from prefect import flow, task, get_run_logger
from kafka import KafkaProducer
from time import sleep
import pandas as pd

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")  
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")
TICKERS = ["TLKM.JK"]
BATCH_SIZE = 1

@task(retries=3, retry_delay_seconds=10)
def fetch_yahoo_data(tickers: list):
    logger = get_run_logger()
    logger.info(f"Fetching data for tickers: {tickers}")
    
    try:
        data = yf.download(
            tickers=tickers,
            period="1d",
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            threads=True,
        )
        
        if data.empty:
            logger.warning("No data returned from yfinance")
            return []
            
    except Exception as e:
        logger.error(f"Failed to download data: {e}")
        return []
    
    records = []
    
    # Process each ticker
    for ticker in tickers:
        try:
            # Handle MultiIndex columns or regular columns
            if isinstance(data.columns, pd.MultiIndex):
                # MultiIndex case - columns are like ('TLKM.JK', 'Open')
                if ticker in data.columns.levels[0]:
                    df = data[ticker]
                else:
                    logger.warning(f"Ticker {ticker} not found in MultiIndex data")
                    continue
            else:
                # Single level columns case
                df = data
            
            if df.empty:
                logger.warning(f"No data for {ticker}")
                continue
                
            # Check if we have the required columns
            required_columns = ["Open", "High", "Low", "Close", "Volume"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.error(f"Missing columns for {ticker}: {missing_columns}")
                logger.info(f"Available columns: {list(df.columns)}")
                continue
                
            latest = df.iloc[-1]
            record = {
                "ticker": ticker,
                "date": pd.to_datetime(latest.name).to_pydatetime(),
                "open": float(latest["Open"]) if pd.notna(latest["Open"]) else 0.0,
                "high": float(latest["High"]) if pd.notna(latest["High"]) else 0.0,
                "low": float(latest["Low"]) if pd.notna(latest["Low"]) else 0.0,
                "close": float(latest["Close"]) if pd.notna(latest["Close"]) else 0.0,
                "volume": int(latest["Volume"]) if pd.notna(latest["Volume"]) else 0,
            }
            records.append(record)
            logger.info(f"Successfully processed {ticker}")
            
        except Exception as e:
            logger.error(f"Failed to process {ticker}: {e}")
            logger.info(f"Data shape: {data.shape}, Columns: {list(data.columns)}")
    
    logger.info(f"Fetched {len(records)} records")
    return records

@task
def publish_to_kafka(records: list):
    logger = get_run_logger()
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json_util.dumps(v).encode("utf-8")
        )
        
        for record in records:
            producer.send(KAFKA_TOPIC, value=record)
            logger.info(f"Published {record['ticker']} to Kafka")
            
        producer.flush()
        producer.close()
        logger.info("All records published and Kafka connection closed.")
        
    except Exception as e:
        logger.error(f"Failed to publish to Kafka: {e}")
        raise

@flow(name="stock-producer-flow")
def stock_producer_flow():
    logger = get_run_logger()
    all_records = []
    
    for i in range(0, len(TICKERS), BATCH_SIZE):
        batch = TICKERS[i:i+BATCH_SIZE]
        logger.info(f"Processing batch: {batch}")
        
        records = fetch_yahoo_data(batch)
        if records:
            all_records.extend(records)
            publish_to_kafka(records)
        else:
            logger.warning(f"No records fetched for batch: {batch}")
            
        if i + BATCH_SIZE < len(TICKERS):  # Don't sleep after the last batch
            sleep(5)
       
    if not all_records:
        logger.warning("No records fetched in this run.")
    else:
        logger.info(f"Successfully processed {len(all_records)} total records")

if __name__ == "__main__":
    stock_producer_flow()