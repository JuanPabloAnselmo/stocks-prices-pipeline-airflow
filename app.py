import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy.engine import Engine
from utils.database import create_redshift_engine
import matplotlib.dates as mdates
from typing import Optional


def plot_stock_data(engine: Engine) -> None:
    """
    Retrieve stock data from a Redshift database
    and plot selected stock variables over time.

    Args:
        engine (Engine): SQLAlchemy engine object to interact
        with the Redshift database.
    """
    # Query to retrieve stock data
    with engine.begin() as connection:
        query = """
            SELECT
                date,
                symbol,
                open_price,
                high_price,
                low_price,
                close_price,
                volume
            FROM "2024_juan_pablo_anselmo_schema".daily_stock_prices_table
        """
        df: pd.DataFrame = pd.read_sql_query(query, connection)

    # Handle the case where no data is available
    if df.empty:
        st.write("No data available, returning 0 for all columns.")
        df = pd.DataFrame({
            'date': [pd.Timestamp('1970-01-01')],
            'symbol': ['N/A'],
            'open_price': [0.0],
            'high_price': [0.0],
            'low_price': [0.0],
            'close_price': [0.0],
            'volume': [0]
        })

    # Display the loaded data in Streamlit
    st.write("Loaded data:")
    st.write(df.tail())

    # Allow user to select a stock symbol from the available options
    symbols = df['symbol'].unique()
    selected_symbol: Optional[str] = st.sidebar.selectbox('Select a symbol', symbols)

    # Filter data for the selected symbol
    filtered_df = df[df['symbol'] == selected_symbol].copy()
    filtered_df['date'] = pd.to_datetime(filtered_df['date'])
    filtered_df = filtered_df.sort_values('date')

    # Select numeric variables for plotting
    numeric_columns = [col for col in filtered_df.columns if pd.api.types.is_numeric_dtype(filtered_df[col])]  # noqa: E501
    selected_variable: Optional[str] = st.sidebar.selectbox('Select a numeric variable', numeric_columns)  # noqa: E501

    st.write(f"Plot of the variable '{selected_variable}' for symbol '{selected_symbol}'")  # noqa: E501

    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(filtered_df['date'], filtered_df[selected_variable], marker='o')
    ax.set_xlabel('Date')
    ax.set_ylabel(selected_variable)
    ax.set_title(f'Evolution of {selected_variable} for {selected_symbol}')
    ax.grid(True)

    # Configure x-axis to show all dates
    ax.xaxis.set_major_locator(mdates.DayLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45, ha='right')

    # Set x-axis limits to cover the entire date range with one day before and after
    ax.set_xlim(filtered_df['date'].min() - pd.Timedelta(days=1), 
                 filtered_df['date'].max() + pd.Timedelta(days=1))

    # Adjust layout for better fit
    fig.tight_layout()

    # Display the plot in Streamlit
    st.pyplot(fig)


if __name__ == "__main__":
    # Create a connection to the Redshift database and plot stock data
    conn: Engine = create_redshift_engine()
    plot_stock_data(conn)
