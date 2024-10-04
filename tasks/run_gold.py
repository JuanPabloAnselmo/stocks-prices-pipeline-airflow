from sqlalchemy.engine import Engine
from utils.database import create_redshift_engine
from gold.calculate_stock_attributes import calculate_stock_attributes


def run_gold(**context) -> None:
    """
    Run the gold layer process, which calculates stock attributes
    and inserts the calculated data into the Redshift database.

    Steps:
        1. Create a connection to the Redshift database.
        2. Calculate stock attributes based on daily stock prices for the given date.
        3. Insert the calculated attributes into the relevant table in Redshift.

    Args:
        None

    """

    conn: Engine = create_redshift_engine()

    # Calculate stock attributes and insert them into Redshift
    calculate_stock_attributes(conn, context["ds"])


if __name__ == "__main__":
    run_gold()
