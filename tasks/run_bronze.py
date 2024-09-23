from bronze.parquet_create import parquet_create
from utils.config import API_KEY_ALPHA, API_KEY_FINHUB,  STOCKS_SYMBOLS_LIST
from airflow.exceptions import AirflowException
from typing import Any


def run_bronze(**context: Any) -> None:
    """
    Executes the bronze layer task, which creates parquet files with stock data
    retrieved from external APIs.

    Args:
        **kwargs (Any): Additional arguments passed from Airflow or the context.

    Raises:
        AirflowException: If the parquet creation process fails, this exception
        is raised to mark the task as failed in the DAG.
    """
    try:
        parquet_create(context["ds"], STOCKS_SYMBOLS_LIST, API_KEY_ALPHA, API_KEY_FINHUB)  # noqa: E501
    except AirflowException as e:
        print(f"Failed in run_bronze: {e}")
        raise e  # Force the task to fail to cancel the DAG


if __name__ == "__main__":
    run_bronze()
