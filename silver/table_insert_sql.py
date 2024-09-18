import pandas as pd
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.engine import Engine


def insert_stock_data_scd2(engine: Engine, stock_df: pd.DataFrame) -> None:
    """
    Implement Slowly Changing Dimension (SCD) Type 2 in the 'stock_table'.

    Args:
        engine (Engine): SQLAlchemy engine for database connection.
        stock_df (pd.DataFrame): DataFrame containing stock data to be inserted.

    Raises:
        Exception: If an error occurs during the database operation.
    """
    with engine.begin() as connection:
        try:
            rows_added = 0
            rows_updated = 0

            for _, row in stock_df.iterrows():
                # Check if there's an existing current record for the symbol
                result = connection.execute(
                    text(
                        """
                    SELECT * FROM "2024_juan_pablo_anselmo_schema".stock_table
                    WHERE symbol = :symbol AND is_current = 1
                """
                    ),
                    {"symbol": row["symbol"]},
                )
                current_record = result.fetchone()

                if current_record:
                    # Check if any values have changed
                    # (excluding start_date, end_date, is_current)
                    if (
                        row["name"] != current_record[2]
                        or row["industry"] != current_record[3]
                        or row["exchange"] != current_record[4]
                        or row["logo"] != current_record[5]
                        or row["weburl"] != current_record[6]
                    ):
                        # Mark the existing record as not current and set the end date
                        connection.execute(
                            text(
                                """
                            UPDATE "2024_juan_pablo_anselmo_schema".stock_table
                            SET is_current = 0, end_date = :end_date
                            WHERE symbol = :symbol AND is_current = 1
                        """
                            ),
                            {
                                "end_date": datetime.now().date(),
                                "symbol": row["symbol"],
                            },
                        )
                        rows_updated += 1

                        # Insert a new record with is_current = 1 and start_date = today
                        connection.execute(
                            text(
                                """
                            INSERT INTO "2024_juan_pablo_anselmo_schema".stock_table (
                                symbol, name, industry, exchange, logo, weburl
                                    , start_date, end_date, is_current
                            ) VALUES (:symbol, :name, :industry, :exchange,
                                :logo, :weburl, :start_date, :end_date, :is_current)
                        """
                            ),
                            {
                                "symbol": row["symbol"],
                                "name": row["name"],
                                "industry": row["industry"],
                                "exchange": row["exchange"],
                                "logo": row["logo"],
                                "weburl": row["weburl"],
                                "start_date": datetime.now().date(),
                                "end_date": datetime.strptime(
                                    "3000-12-01", "%Y-%m-%d"
                                ).date(),
                                "is_current": 1,
                            },
                        )
                        rows_added += 1
                else:
                    # Insert a new record if no current record exists
                    connection.execute(
                        text(
                            """
                        INSERT INTO "2024_juan_pablo_anselmo_schema".stock_table (
                            symbol, name, industry, exchange
                                , logo, weburl, start_date, end_date, is_current
                        ) VALUES (:symbol, :name, :industry, :exchange,
                            :logo, :weburl, :start_date, :end_date, :is_current)
                    """
                        ),
                        {
                            "symbol": row["symbol"],
                            "name": row["name"],
                            "industry": row["industry"],
                            "exchange": row["exchange"],
                            "logo": row["logo"],
                            "weburl": row["weburl"],
                            "start_date": datetime.now().date(),
                            "end_date": datetime.strptime(
                                "3000-12-01", "%Y-%m-%d"
                            ).date(),
                            "is_current": 1,
                        },
                    )
                    rows_added += 1

            # Notify how many records were added or updated
            if rows_updated > 0:
                print(f"Updated {rows_updated} records in stock_table.")
            if rows_added > 0:
                print(f"Added {rows_added} new records to stock_table.")
            if rows_added == 0 and rows_updated == 0:
                print("No records were added or updated in stock_table.")

        except Exception as e:
            print(f"Error inserting data into stock_table: {e}")
            raise


def insert_date_data(engine: Engine, date_df: pd.DataFrame) -> None:
    """
    Insert or update date data in the 'date_table'.

    Args:
        engine (Engine): SQLAlchemy engine for database connection.
        date_df (pd.DataFrame): DataFrame containing date data to be inserted.

    Raises:
        Exception: If an error occurs during the database operation.
    """
    with engine.connect() as connection:
        try:
            # Get the latest date from the date_table
            result = connection.execute(
                text(
                    """
                SELECT MAX(date) FROM "2024_juan_pablo_anselmo_schema".date_table
            """
                )
            )
            max_date_in_table = result.fetchone()[0]

            # Convert the date from the table to date type if it's not None
            if max_date_in_table is not None:
                max_date_in_table = pd.to_datetime(max_date_in_table).date()

            # Ensure the 'date' column in date_df matches the database date type
            date_df["date"] = pd.to_datetime(date_df["date"]).dt.date

            # Get the latest date in the DataFrame
            max_date_in_df = date_df["date"].max()

            # Check if the latest date in the DataFrame already exists in the table
            if max_date_in_table is None or max_date_in_df > max_date_in_table:
                # Filter new dates
                new_dates_df = (
                    date_df[date_df["date"] > max_date_in_table]
                    if max_date_in_table
                    else date_df
                )

                # Insert new dates into the table
                new_dates_df.to_sql(
                    "date_table",
                    con=connection,
                    schema="2024_juan_pablo_anselmo_schema",
                    if_exists="append",
                    index=False,
                )
                print(f"Added {len(new_dates_df)} new dates to date_table.")
            else:
                print(
                    "No new dates were added; they were already present in date_table."
                )

        except Exception as e:
            print(f"Error inserting data into date_table: {e}")
            raise
            # The transaction will be automatically rolled back
            # when leaving the 'begin()' block


def insert_stock_prices_data(
    engine: Engine, daily_stock_prices_df: pd.DataFrame
) -> None:
    """
    Insert or update daily stock prices data in the 'daily_stock_prices_table'.

    Args:
        engine (Engine): SQLAlchemy engine for database connection.
        daily_stock_prices_df (pd.DataFrame): DataFrame containing
            daily stock prices data to be inserted.

    Raises:
        Exception: If an error occurs during the database operation.
    """
    with engine.begin() as connection:
        try:
            # Get the latest date from the daily_stock_prices_table
            result = connection.execute(
                text(
                    """
                SELECT MAX(date)
                FROM "2024_juan_pablo_anselmo_schema".daily_stock_prices_table
            """
                )
            )
            max_date_in_prices_table = result.fetchone()[0]

            # Convert the date from the table to date type if it's not None
            if max_date_in_prices_table is not None:
                max_date_in_prices_table = pd.to_datetime(
                    max_date_in_prices_table
                ).date()

            # Ensure the 'date' column in daily_stock_prices_df matches
            # the database date type
            daily_stock_prices_df["date"] = pd.to_datetime(
                daily_stock_prices_df["date"]
            ).dt.date

            # Get the latest date in the daily stock prices DataFrame
            max_date_in_prices_df = daily_stock_prices_df["date"].max()

            # Check if the latest date in the DataFrame already exists in the table
            if (
                max_date_in_prices_table is None
                or max_date_in_prices_df > max_date_in_prices_table
            ):
                # Filter new records
                new_prices_df = (
                    daily_stock_prices_df[
                        daily_stock_prices_df["date"] > max_date_in_prices_table
                    ]
                    if max_date_in_prices_table
                    else daily_stock_prices_df
                )

                # Insert new records into the daily stock prices table
                new_prices_df.to_sql(
                    "daily_stock_prices_table",
                    con=connection,
                    schema="2024_juan_pablo_anselmo_schema",
                    if_exists="append",
                    index=False,
                )
                print(
                    f"Added {len(new_prices_df)} records to daily_stock_prices_table."
                )
            else:
                print(
                    "No new records were added; \
they were already present in daily_stock_prices_table."
                )

        except Exception as e:
            print(f"Error inserting data into daily_stock_prices_table: {e}")
            raise
