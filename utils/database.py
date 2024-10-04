import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from utils.config import (
    DBNAME_REDSHIFT, USER_REDSHIFT, PASSWORD_REDSHIFT,
    HOST_REDSHIFT, PORT_REDSHIFT
)


# Connection variables for Redshift
dbname: str = DBNAME_REDSHIFT
user: str = USER_REDSHIFT
# Handle special characters in password
password: str = urllib.parse.quote_plus(str(PASSWORD_REDSHIFT))
host: str = HOST_REDSHIFT
port: str = PORT_REDSHIFT


def create_redshift_engine() -> Engine:
    """
    Create a SQLAlchemy engine for connecting to a Redshift database.

    This function builds a connection string using the provided credentials
    and returns a SQLAlchemy engine that can be used to interact with the database.

    Returns:
        Engine: A SQLAlchemy Engine object connected to the Redshift database.
    """

    # Create a SQLAlchemy engine for Redshift connection
    engine: Engine = create_engine(
        f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'
    )
    return engine

