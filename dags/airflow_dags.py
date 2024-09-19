import os
import sys
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tasks.run_bronze import run_bronze  # noqa: E402
from tasks.run_silver import run_silver  # noqa: E402
from tasks.run_gold import run_gold  # noqa: E402

# Default arguments for the DAG
default_args = {
    "owner": "jpanselmo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),  # Retry delay set to 5 minutes
}

# Define the DAG
with DAG(
    dag_id="stock_price_dags",
    default_args=default_args,
    description="DAG to process stock price data and load it into Redshift",
    schedule_interval="0 0 * * 2-6",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Task to extract data from the API and generate Parquet files (Bronze layer)
    bronze_task = PythonOperator(
        task_id="bronze_run",
        python_callable=run_bronze,
        provide_context=True,  # Allows passing context if needed in the function
    )

    # Task to load Parquet files into Redshift (Silver layer)
    silver_task = PythonOperator(
        task_id="silver_run",
        python_callable=run_silver,
        provide_context=True,
    )

    # Task to create the final attributes table (Gold layer)
    gold_task = PythonOperator(
        task_id="gold_run",
        python_callable=run_gold,
        provide_context=True,
    )

    # Define task execution sequence
    bronze_task >> silver_task >> gold_task
