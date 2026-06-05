from utils.logger import get_logger
from utils.db import get_db_connection

logger = get_logger("MartVerifier")

def verify_data_marts() -> None:
    # Runs query to verify data marts are populated.
    logger.info("Verifying data marts readiness...")
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), ticker FROM mart_stock_summary GROUP BY ticker;")
            results = cur.fetchall()
            for count, ticker in results:
                logger.info(f"Data mart verified: Ticker {ticker} has {count} rows in summary.")
    except Exception as e:
        logger.error(f"Failed to verify data marts: {e}")
        raise
    finally:
        conn.close()
