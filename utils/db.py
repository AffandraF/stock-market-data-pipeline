import psycopg2
from utils.logger import get_logger
from utils.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

logger = get_logger("DB")

def get_db_connection():
    # Connects to PostgreSQL using psycopg2.
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        # Set default schema search path to stock_data
        with conn.cursor() as cur:
            cur.execute("SET search_path TO stock_data, public;")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise
