import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from utils.config import REDSHIFT_SCHEMA


def calculate_stock_attributes(engine: Engine, date: str) -> None:
    """
    Calculate financial attributes for the 'gold' layer based on stock data and insert
    the results into the 'atributes_stock_prices_table' in the database. If data for
    a given id_transaction already exists, it will be overwritten.

    Args:
        engine (Engine): SQLAlchemy engine for database connection.
        date (str): Date for which the stock attributes are calculated.

    Raises:
        Exception: If there is an issue with the database query or insertion.
    """

    with engine.begin() as connection:
        # Read data from daily_stock_prices_table for the given date
        query = text(f"""
            SELECT
                id_transaction,
                date,
                symbol,
                open_price,
                high_price,
                low_price,
                close_price,
                volume
            FROM "{REDSHIFT_SCHEMA}".daily_stock_prices_table
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

        # Delete existing rows for the same id_transaction before inserting
        delete_query = text(f"""
            DELETE FROM "{REDSHIFT_SCHEMA}".atributes_stock_prices_table
            WHERE id_transaction IN :id_transactions
        """)
        connection.execute(delete_query, {'id_transactions': tuple(df['id_transaction'].tolist())})  # noqa: E501

        # Insert calculated attributes into the 'gold' table
        df.to_sql(
            'atributes_stock_prices_table',
            con=connection,
            schema=f'{REDSHIFT_SCHEMA}',
            if_exists='append',
            index=False
        )

        # Log the successful insertion of calculated attributes
        print(f"Attributes calculated and successfully inserted for the date {date}.")  # noqa: E501
