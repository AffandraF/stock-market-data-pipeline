import os
import sys
from datetime import datetime, timedelta

# Dynamically add the project root and src directory to sys.path
# to ensure Airflow can resolve imports properly
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if project_root not in sys.path:
    sys.path.append(project_root)
    sys.path.append(os.path.join(project_root, "src"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.config import STOCK_TICKERS
from utils.init_db import init_database
from extract.extractor import extract_stock
from validate.validator import validate_stock
from transform.transformer import transform_stock
from load.loader import load_stock
from utils.logger import get_logger

logger = get_logger("AirflowDAG")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def run_db_init() -> None:
    """
    Initializes PostgreSQL tables and views.
    """
    logger.info("Initializing database schemas and marts views...")
    init_database()

def verify_data_marts() -> None:
    """
    Runs a validation query against the marts to verify they are ready and populated.
    """
    from utils.db import get_db_connection
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

with DAG(
    "stock_market_etl_pipeline",
    default_args=default_args,
    description="End-to-end batch stock market ETL pipeline with Pandas and PostgreSQL star schema",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. Initialize Database Schema & Marts Views
    db_init = PythonOperator(
        task_id="initialize_database",
        python_callable=run_db_init,
    )

    # 5. Build/Verify Data Marts (runs after all loads complete)
    verify_marts = PythonOperator(
        task_id="verify_data_marts",
        python_callable=verify_data_marts,
    )

    # Create ETL pipeline tasks for each stock ticker
    for ticker in STOCK_TICKERS:
        # Define tasks for each step
        extract_task = PythonOperator(
            task_id=f"extract_{ticker}",
            python_callable=extract_stock,
            op_kwargs={"ticker": ticker},
        )

        validate_task = PythonOperator(
            task_id=f"validate_{ticker}",
            python_callable=validate_stock,
            op_kwargs={"ticker": ticker},
        )

        transform_task = PythonOperator(
            task_id=f"transform_{ticker}",
            python_callable=transform_stock,
            op_kwargs={"ticker": ticker},
        )

        load_task = PythonOperator(
            task_id=f"load_{ticker}",
            python_callable=load_stock,
            op_kwargs={"ticker": ticker},
        )

        # Build task dependencies per ticker:
        # db_init -> Extract -> Validate -> Transform -> Load -> verify_marts
        db_init >> extract_task >> validate_task >> transform_task >> load_task >> verify_marts
