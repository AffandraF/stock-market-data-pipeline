from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract import extract_stock
from src.load import load_stock
from src.transform import transform_stock
from utils.config import STOCK_TICKERS
from utils.init_db import run_db_init
from src.validate import validate_stock
from utils.mart_verifier import verify_data_marts

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "stock_market_etl_pipeline",
    default_args=default_args,
    description="End-to-end batch stock market ETL pipeline with Pandas and PostgreSQL star schema",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
) as dag:

    db_init = PythonOperator(
        task_id="initialize_database",
        python_callable=run_db_init,
    )

    verify_marts = PythonOperator(
        task_id="verify_data_marts",
        python_callable=verify_data_marts,
    )

    for ticker in STOCK_TICKERS:
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

        db_init >> extract_task >> validate_task >> transform_task >> load_task >> verify_marts
