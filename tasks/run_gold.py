from sqlalchemy.engine import Engine
from utils.database import create_redshift_engine
from gold.calculate_stock_attributes import calculate_stock_attributes
from utils.config import DATE_STR


def run_gold() -> None:
    """
    Run the gold layer process, which calculates stock attributes
    and inserts the calculated data into the Redshift database.

    Steps:
        1. Create a connection to the Redshift database.
        2. Calculate stock attributes based on daily stock prices for the given date.
        3. Insert the calculated attributes into the relevant table in Redshift.

    Args:
        None

    Raises:
        Exception: If there is an issue with calculating stock attributes
        or the database connection.
    """
    try:
        conn: Engine = create_redshift_engine()

        # Calculate stock attributes and insert them into Redshift
        calculate_stock_attributes(conn, DATE_STR)

    except Exception as e:
        # Print the exception and re-raise it
        print(f"An error occurred: {e}")
        raise e


if __name__ == "__main__":
    run_gold()
