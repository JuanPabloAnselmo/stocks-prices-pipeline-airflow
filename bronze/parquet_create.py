import os
from typing import List
import pandas as pd
from airflow.exceptions import AirflowException
from bronze.api_data_downloader import (
    create_stock_table,
    create_daily_stock_prices_table,
)
from utils.config import DIR_PATH


def parquet_create(
    date: str, stock_symbols: List[str], api_key_alpha: str, api_key_finnhub: str
) -> None:
    """
    Creates Parquet files for daily stock prices and stock profiles.

    Args:
        date (str): The date for which the data is retrieved, in 'YYYY-MM-DD' format.
        stock_symbols (List[str]): A list of stock symbols to retrieve data for.
        api_key_alpha (str): The Alpha Vantage API key.
        api_key_finnhub (str): The Finnhub API key.

    Raises:
        AirflowException: If no valid data is retrieved for the given symbols.
    """
    try:
        # Create DataFrame for daily stock prices
        daily_stock_prices_table: pd.DataFrame = pd.concat(
            [
                create_daily_stock_prices_table(symbol, date, api_key_alpha)
                for symbol in stock_symbols
            ],
            ignore_index=True,
        )

        # Check if the DataFrame is empty
        if daily_stock_prices_table.empty:
            raise ValueError(
                "Failed to retrieve daily stock prices for the provided symbols."
            )

    except Exception as e:
        print(f"Error retrieving daily stock prices: {e}")
        daily_stock_prices_table = pd.DataFrame()  # Empty DataFrame in case of error

    try:
        # Create DataFrame for stock profiles
        stock_table: pd.DataFrame = pd.concat(
            [create_stock_table(symbol, api_key_finnhub) for symbol in stock_symbols],
            ignore_index=True,
        )

        # Check if the DataFrame is empty
        if stock_table.empty:
            raise ValueError(
                "Failed to retrieve stock profile information for the provided symbols."
            )

    except Exception as e:
        print(f"Error retrieving stock profile information: {e}")
        stock_table = pd.DataFrame()  # Empty DataFrame in case of error

    # Validate that daily_stock_prices_table DataFrame have data
    # before saving to Parquet files
    if daily_stock_prices_table.empty:
        raise AirflowException(
            "No valid data was retrieved from the API. Cancel the DAG."
        )

    # Save daily stock prices DataFrame to a Parquet file
    try:
        daily_stock_prices_file: str = os.path.join(
            DIR_PATH,
            "bronze",
            "data",
            f"daily_stock_prices_table_{date}_bronze.parquet",
        )
        daily_stock_prices_table.to_parquet(daily_stock_prices_file, index=False)
        print(f"File '{daily_stock_prices_file}' created successfully.")
    except Exception as e:
        print(f"Error saving '{daily_stock_prices_file}': {e}")

    # Save stock profile DataFrame to a Parquet file
    try:
        stock_table_file: str = os.path.join(
            DIR_PATH, "bronze", "data", f"stock_table_{date}_bronze.parquet"
        )
        stock_table.to_parquet(stock_table_file, index=False)
        print(f"File '{stock_table_file}' created successfully.")
    except Exception as e:
        print(f"Error saving '{stock_table_file}': {e}")
