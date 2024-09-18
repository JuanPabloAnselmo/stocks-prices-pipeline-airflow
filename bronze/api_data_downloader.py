import pandas as pd
import requests
from typing import Dict, Optional


def create_daily_stock_prices_table(
    symbol: str, date: str, api_key: str
) -> pd.DataFrame:
    """
    Fetches daily stock prices from the Alpha Vantage API.

    Args:
        symbol (str): The stock symbol for which prices are retrieved.
        date (str): The date for which prices are retrieved, in 'YYYY-MM-DD' format.
        api_key (str): The Alpha Vantage API key.

    Returns:
        pd.DataFrame: A DataFrame containing open, high, low, close prices, and volume
                      for the specified date. Returns an empty DataFrame if data cannot
                      be retrieved or if no data is available for the given date.
    """
    url: str = (
        "https://www.alphavantage.co/query"
        "?function=TIME_SERIES_DAILY"
        f"&symbol={symbol}&apikey={api_key}&outputsize=compact"
    )

    try:
        response: requests.Response = requests.get(url)
        response.raise_for_status()
        data: Dict = response.json()

        if "Information" in data:
            print(f"Alpha Vantage API Error: {data['Information']}")
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"Error making request to Alpha Vantage API: {e}")
        return pd.DataFrame()

    daily_prices: Optional[Dict] = data.get("Time Series (Daily)", {})
    if not daily_prices:
        print(f"No price data found for symbol {symbol} on date {date}.")
        return pd.DataFrame()

    price_info: Optional[Dict] = daily_prices.get(date, {})

    if price_info:
        rows = [
            {
                "date": date,
                "stock_symbol": symbol,
                "open_price": float(price_info.get("1. open", 0)),
                "high_price": float(price_info.get("2. high", 0)),
                "low_price": float(price_info.get("3. low", 0)),
                "close_price": float(price_info.get("4. close", 0)),
                "volume": float(price_info.get("5. volume", 0)),
            }
        ]
    else:
        print(f"No price information available for {symbol} on date {date}.")
        return pd.DataFrame()

    return pd.DataFrame(rows)


def create_stock_table(symbol: str, api_key: str) -> pd.DataFrame:
    """
    Fetches stock profile information from the Finnhub API.

    Args:
        symbol (str): The stock symbol for which the profile is retrieved.
        api_key (str): The Finnhub API key.

    Returns:
        pd.DataFrame: A DataFrame with the stock profile including name, industry, etc.
                      Returns an empty DataFrame if data cannot be retrieved or if no
                      data is available for the given symbol.
    """
    url: str = (
        "https://finnhub.io/api/v1/stock/profile2"
        f"?symbol={symbol}&token={api_key}"
    )

    try:
        response: requests.Response = requests.get(url)
        response.raise_for_status()
        data: Dict = response.json()

        if not data or data.get("error"):
            print(f"Finnhub API Error: {data.get('error', 'Data not available.')}")
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"Error making request to Finnhub API: {e}")
        return pd.DataFrame()

    if not data:
        print(f"No profile data found for symbol {symbol}.")
        return pd.DataFrame()

    rows = [
        {
            "symbol": data.get("ticker", ""),
            "name": data.get("name", ""),
            "industry": data.get("finnhubIndustry", ""),
            "exchange": data.get("exchange", ""),
            "logo": data.get("logo", ""),
            "weburl": data.get("weburl", ""),
        }
    ]

    return pd.DataFrame(rows)
