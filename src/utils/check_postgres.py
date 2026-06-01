import os
import pandas as pd
from utils.db import get_db_connection
from utils.logger import get_logger

logger = get_logger("CheckPostgres")

def fetch_stock_summary() -> pd.DataFrame:
    """
    Fetches the flat stock summary from the mart_stock_summary view in PostgreSQL.
    """
    logger.info("Fetching flat stock summary from PostgreSQL...")
    conn = get_db_connection()
    try:
        query = "SELECT * FROM mart_stock_summary ORDER BY date DESC, ticker ASC;"
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        df = fetch_stock_summary()
        logger.info(f"Retrieved {len(df)} rows from mart_stock_summary.")
        
        # Display sample and basic info
        print("\n--- Data Mart Sample ---")
        print(df.head(5))
        print("\n--- Columns and Types ---")
        print(df.info())
        
        # Save to CSV as before
        output_csv = "data/stock_data_processed.csv"
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        df.to_csv(output_csv, index=False)
        logger.info(f"Successfully saved stock summary to {output_csv}")
        
    except Exception as e:
        logger.error(f"Error checking PostgreSQL data mart: {e}")
