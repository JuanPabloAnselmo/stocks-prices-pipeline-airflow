import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy.engine import Engine
from utils.database import create_redshift_engine


def plot_stock_data(engine: Engine) -> None:
    """
    Fetches stock data from the Redshift database and plots it using Streamlit.

    If the data is empty, it will return 0 for all columns.

    Args:
        engine (Engine): SQLAlchemy engine connected to the Redshift database.
    """
    with engine.begin() as connection:
        # Load the data from the database
        query = """
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
        """
        df = pd.read_sql_query(query, connection)

        # Handle empty dataframe by filling it with zeros
        if df.empty:
            st.write("No data available, returning 0 for all columns.")
            df = pd.DataFrame({
                'id_transaction': [0],
                'date': [pd.Timestamp('1970-01-01')],
                'symbol': ['N/A'],
                'open_price': [0.0],
                'high_price': [0.0],
                'low_price': [0.0],
                'close_price': [0.0],
                'volume': [0]
            })

        # Display a summary of the data
        st.write("Loaded data:")
        st.write(df.head())

        # Select the stock symbol
        symbols = df['symbol'].unique()
        selected_symbol = st.sidebar.selectbox('Select a symbol', symbols)

        # Filter data based on the selected symbol
        filtered_df = df[df['symbol'] == selected_symbol]

        # Select the numeric variable to plot
        numeric_columns = [
            col for col in filtered_df.columns
            if pd.api.types.is_numeric_dtype(filtered_df[col])
        ]
        selected_variable = st.sidebar.selectbox(
            'Select a numeric variable',
            numeric_columns
        )

        # Plot the data
        st.write(
            f"Plot of the variable '{selected_variable}' for symbol '{selected_symbol}'"
        )

        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(filtered_df['date'], filtered_df[selected_variable], marker='o')
        ax.set_xlabel('Date')
        ax.set_ylabel(selected_variable)
        ax.set_title(f'Evolution of {selected_variable} for {selected_symbol}')
        ax.grid(True)

        plt.xticks(rotation=45, ha='right')

        st.pyplot(fig)


if __name__ == "__main__":
    conn = create_redshift_engine()
    plot_stock_data(conn)
