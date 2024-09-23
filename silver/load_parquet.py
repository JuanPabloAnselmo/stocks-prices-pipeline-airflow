import pandas as pd
import os
from typing import Tuple
from utils.config import DIR_PATH


def load_parquet_files(date: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load and update Parquet files for daily stock prices, stock data, and dates.

    Args:
        date (str): The date string used for identifying the Parquet files.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        DataFrames for daily stock prices, stock data, and date information.

    Raises:
        Exception: If there is an error in processing the Parquet files.
    """

    # Paths for daily stock prices
    daily_stock_prices_path = os.path.join(
        DIR_PATH,
        "bronze",
        "data",
        f"daily_stock_prices_table_{date}_bronze.parquet",
    )
    daily_silver_path = os.path.join(
        DIR_PATH, "silver", "data", "daily_stock_prices_table_silver.parquet"
    )

    # Load daily stock prices DataFrame
    daily_stock_prices_df = pd.read_parquet(daily_stock_prices_path)
    daily_stock_prices_df = daily_stock_prices_df.rename(
        columns={"stock_symbol": "symbol"}
    )

    # Check if Silver file exists and update it
    if os.path.exists(daily_silver_path):
        existing_daily_df = pd.read_parquet(daily_silver_path)
        new_data = daily_stock_prices_df[
            ~daily_stock_prices_df["date"].isin(existing_daily_df["date"])
        ]

        if not new_data.empty:
            daily_stock_prices_df = pd.concat(
                [existing_daily_df, new_data], ignore_index=True
            )
            daily_stock_prices_df.to_parquet(daily_silver_path, index=False)
            print("New data added to daily_stock_prices_table_silver.")
        else:
            print(
                "No new data added to daily_stock_prices_table_silver; \
data for these dates already exists."
            )
    else:
        daily_stock_prices_df.to_parquet(daily_silver_path, index=False)
        print("File daily_stock_prices_table_silver created with initial data.")

    # Paths for stock data
    stock_path = os.path.join(
        DIR_PATH, "bronze", "data", f"stock_table_{date}_bronze.parquet"
    )
    stock_silver_path = os.path.join(
        DIR_PATH, "silver", "data", "stock_table_silver.parquet"
    )

    # Load stock data DataFrame
    stock_df = pd.read_parquet(stock_path)

    # Check if Silver file exists and update it
    if os.path.exists(stock_silver_path):
        existing_stock_df = pd.read_parquet(stock_silver_path)
        new_data = pd.merge(stock_df, existing_stock_df, how="left", indicator=True)
        new_data = new_data[new_data["_merge"] == "left_only"].drop(
            columns=["_merge"]
        )

        if not new_data.empty:
            stock_df = pd.concat([existing_stock_df, new_data], ignore_index=True)
            stock_df.to_parquet(stock_silver_path, index=False)
            print("New data added to stock_table_silver.")
        else:
            print(
                "No new data added to stock_table_silver; \
data for these rows already exists."
            )
    else:
        stock_df.to_parquet(stock_silver_path, index=False)
        print("File stock_table_silver created with initial data.")

    # Generate date DataFrame
    unique_dates = pd.to_datetime(daily_stock_prices_df["date"]).dt.date.unique()
    date_df = pd.DataFrame(pd.to_datetime(unique_dates), columns=["date"])
    date_df["day_of_week"] = date_df["date"].apply(lambda x: x.strftime("%A"))
    date_df["day_of_week_short"] = date_df["date"].apply(lambda x: x.strftime("%a"))
    date_df["day_of_month"] = date_df["date"].apply(lambda x: x.day)
    date_df["day_of_year"] = date_df["date"].apply(lambda x: x.timetuple().tm_yday)
    date_df["week_of_year"] = date_df["date"].apply(lambda x: x.isocalendar()[1])
    date_df["month"] = date_df["date"].apply(lambda x: x.strftime("%B"))
    date_df["month_short"] = date_df["date"].apply(lambda x: x.strftime("%b"))
    date_df["month_number"] = date_df["date"].apply(lambda x: x.month)
    date_df["quarter"] = date_df["date"].apply(lambda x: (x.month - 1) // 3 + 1)
    date_df["year"] = date_df["date"].apply(lambda x: x.year)
    date_df["is_weekend"] = date_df["date"].apply(
        lambda x: 1 if x.weekday() >= 5 else 0
    )

    # Paths for date table
    date_silver_path = os.path.join(
        DIR_PATH, "silver", "data", "date_table_silver.parquet"
    )

    # Check if Silver file exists and update it
    if os.path.exists(date_silver_path):
        existing_date_df = pd.read_parquet(date_silver_path)
        new_dates = date_df[~date_df["date"].isin(existing_date_df["date"])]

        if not new_dates.empty:
            date_df = pd.concat([existing_date_df, new_dates], ignore_index=True)
            date_df.to_parquet(date_silver_path, index=False)
            print("New dates added to date_table_silver.")
        else:
            print("No new dates added to date_table_silver; dates already exist.")
    else:
        date_df.to_parquet(date_silver_path, index=False)
        print("File date_table_silver created with initial dates.")

    return daily_stock_prices_df, stock_df, date_df
