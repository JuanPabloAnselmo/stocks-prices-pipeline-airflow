import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def calculate_stock_attributes(engine: Engine, date: str) -> None:
    """
    Calculate financial attributes for the 'gold' layer based on stock data and insert
    the results into the 'atributes_stock_prices_table' in the database.

    Args:
        engine (Engine): SQLAlchemy engine for database connection.
        date (str): Date for which the stock attributes are calculated.

    Raises:
        Exception: If there is an issue with the database query or insertion.
    """
    try:
        with engine.begin() as connection:
            # Read data from daily_stock_prices_table for the given date
            query = text("""
                SELECT
                    id_transaction,
                    date,
                    symbol,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume
                FROM "2024_juan_pablo_anselmo_schema".daily_stock_prices_table
                WHERE date = :date
            """)
            df = pd.read_sql_query(query, connection, params={'date': date})

            if df.empty:
                # If no data is available for the given date
                print(f"No data available for the date {date}.")
                return

            # Calculate attributes for the stock data
            df['price_range'] = df['high_price'] - df['low_price']
            df['price_change'] = df['close_price'] - df['open_price']
            df['price_change_pct'] = (df['price_change'] / df['open_price']) * 100
            df['high_open_diff'] = df['high_price'] - df['open_price']
            df['low_close_diff'] = df['low_price'] - df['close_price']
            df['volume_change'] = df['volume'].pct_change().fillna(0)
            df['volume_moving_avg'] = df['volume'].rolling(window=5).mean().fillna(0)
            df['price_volatility'] = df['price_range'] / df['close_price']

            # Drop unnecessary columns
            df = df.drop(columns=[
                "open_price", "high_price", "low_price",
                "close_price", "volume"
            ])

            # Insert calculated attributes into the 'gold' table
            df.to_sql(
                'atributes_stock_prices_table',
                con=connection,
                schema='2024_juan_pablo_anselmo_schema',
                if_exists='append',
                index=False
            )

            # Log the successful insertion of calculated attributes
            print(f"Attributes calculated and successfully inserted for the date {date}.")  # noqa: E501

    except Exception as e:
        # Print the exception and re-raise it to ensure the error is propagated
        print(f"An error occurred during attribute calculation: {e}")
        raise e
