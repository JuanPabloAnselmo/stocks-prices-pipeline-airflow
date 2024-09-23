from sqlalchemy import text
from sqlalchemy.engine import Engine


def create_tables(engine: Engine) -> None:
    """
    Create tables in the Redshift database if they do not exist.

    Args:
        engine (Engine): SQLAlchemy engine for the database connection.
    """

    def table_exists(table_name: str) -> bool:
        """
        Check if a table exists in the database schema.

        Args:
            table_name (str): Name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """

        with engine.connect() as connection:
            query = text(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = '2024_juan_pablo_anselmo_schema'
                    AND table_name = :table_name
                )
                """
            )
            result = connection.execute(query, {"table_name": table_name})
            return result.fetchone()[0]

    with engine.connect() as connection:
        # Create stock_table if it does not exist
        if not table_exists("stock_table"):
            connection.execute(
                text(
                    """
                    CREATE TABLE "2024_juan_pablo_anselmo_schema".stock_table (
                        id_record BIGINT IDENTITY(1,1) PRIMARY KEY,
                        symbol VARCHAR(255) UNIQUE,
                        name VARCHAR(255),
                        industry VARCHAR(255),
                        exchange VARCHAR(255),
                        logo VARCHAR(255),
                        weburl VARCHAR(255),
                        start_date DATE,
                        end_date DATE,
                        is_current INTEGER
                    );
                    """
                )
            )
            print("Table 'stock_table' created successfully.")
        else:
            print("Table 'stock_table' already exists.")

        # Create date_table if it does not exist
        if not table_exists("date_table"):
            connection.execute(
                text(
                    """
                    CREATE TABLE "2024_juan_pablo_anselmo_schema".date_table (
                        date DATE PRIMARY KEY,
                        day_of_week TEXT,
                        day_of_week_short TEXT,
                        day_of_month INTEGER,
                        day_of_year INTEGER,
                        week_of_year INTEGER,
                        month TEXT,
                        month_short TEXT,
                        month_number INTEGER,
                        quarter INTEGER,
                        year INTEGER,
                        is_weekend INTEGER
                    );
                    """
                )
            )
            print("Table 'date_table' created successfully.")
        else:
            print("Table 'date_table' already exists.")

        # Create daily_stock_prices_table if it does not exist
        if not table_exists("daily_stock_prices_table"):
            connection.execute(
                text(
                    """
                    CREATE TABLE
                        "2024_juan_pablo_anselmo_schema".daily_stock_prices_table (
                        id_transaction BIGINT IDENTITY(1,1) PRIMARY KEY,
                        date DATE,
                        symbol TEXT,
                        open_price REAL,
                        high_price REAL,
                        low_price REAL,
                        close_price REAL,
                        volume INTEGER,
                        FOREIGN KEY (date)
                            REFERENCES
                            "2024_juan_pablo_anselmo_schema".date_table(date),
                        FOREIGN KEY (symbol)
                            REFERENCES
                            "2024_juan_pablo_anselmo_schema".stock_table(symbol)
                    );
                    """
                )
            )
            print("Table 'daily_stock_prices_table' created successfully.")
        else:
            print("Table 'daily_stock_prices_table' already exists.")

        # Create atributes_stock_prices_table if it does not exist
        if not table_exists("atributes_stock_prices_table"):
            connection.execute(
                text(
                    """
                    CREATE TABLE
                    "2024_juan_pablo_anselmo_schema".atributes_stock_prices_table (
                        id BIGINT IDENTITY(1,1) PRIMARY KEY,
                        id_transaction BIGINT,
                        date DATE,
                        symbol VARCHAR(10),
                        price_range FLOAT,
                        price_change FLOAT,
                        price_change_pct FLOAT,
                        high_open_diff FLOAT,
                        low_close_diff FLOAT,
                        volume_change FLOAT,
                        volume_moving_avg FLOAT,
                        price_volatility FLOAT,
                        FOREIGN KEY (symbol)
                            REFERENCES
                            "2024_juan_pablo_anselmo_schema".stock_table(symbol),
                        FOREIGN KEY (id_transaction)
                            REFERENCES
                            "2024_juan_pablo_anselmo_schema".daily_stock_prices_table
                                (id_transaction)
                    );
                    """
                )
            )
            print("Table 'atributes_stock_prices_table' created successfully.")
        else:
            print("Table 'atributes_stock_prices_table' already exists.")
