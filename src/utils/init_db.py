import os
from utils.logger import get_logger
from utils.db import get_db_connection

logger = get_logger("InitDB")

def run_sql_file(file_path: str, conn) -> None:
    """
    Executes a SQL file on the given database connection.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")
        
    logger.info(f"Executing SQL file: {file_path}...")
    with open(file_path, "r") as f:
        sql = f.read()
        
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info(f"Successfully executed: {file_path}")

def init_database() -> None:
    """
    Initializes the PostgreSQL database with schemas and views.
    """
    conn = get_db_connection()
    try:
        # Run table creation
        run_sql_file("sql/init_warehouse.sql", conn)
        # Run marts view creation
        run_sql_file("sql/marts.sql", conn)
        logger.info("Database initialization completed successfully.")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    init_database()
