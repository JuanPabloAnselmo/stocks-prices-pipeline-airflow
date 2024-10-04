import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

# Add the parent directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bronze.api_data_downloader import create_daily_stock_prices_table  # Ajusta el nombre del módulo donde está la función


class TestCreateDailyStockPricesTable(unittest.TestCase):
    """
    Unit tests for the function 'create_daily_stock_prices_table' from the bronze.api_data_downloader module.
    """

    @patch('bronze.api_data_downloader.requests.get')  # Mock the requests.get call
    def test_valid_response(self, mock_get: MagicMock) -> None:
        """
        Test when the API returns valid data for the specified date.
        """
        # Simulate a valid API response with data for the given date
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "Time Series (Daily)": {
                "2024-09-10": {
                    "1. open": "150.00",
                    "2. high": "155.00",
                    "3. low": "148.00",
                    "4. close": "152.00",
                    "5. volume": "1200000"
                }
            }
        }

        mock_get.return_value = mock_response

        # Call the function with mock data
        df = create_daily_stock_prices_table('AAPL', '2024-09-10', 'dummy_api_key')

        # Expected DataFrame
        expected_data = pd.DataFrame([{
            'date': '2024-09-10',
            'stock_symbol': 'AAPL',
            'open_price': 150.00,
            'high_price': 155.00,
            'low_price': 148.00,
            'close_price': 152.00,
            'volume': 1200000.0
        }])

        # Validate the resulting DataFrame
        pd.testing.assert_frame_equal(df, expected_data)

    @patch('bronze.api_data_downloader.requests.get')
    def test_no_data_for_date(self, mock_get: MagicMock) -> None:
        """
        Test when there is no data for the given date in the API response.
        """
        # Simulate an API response with no data for the requested date
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "Time Series (Daily)": {}
        }

        mock_get.return_value = mock_response

        # Call the function with mock data
        df = create_daily_stock_prices_table('AAPL', '2024-09-10', 'dummy_api_key')

        # Check if the DataFrame is empty as expected
        self.assertTrue(df.empty)

    @patch('bronze.api_data_downloader.requests.get')
    def test_api_error(self, mock_get: MagicMock) -> None:
        """
        Test when the API returns an error (e.g., 500 Internal Server Error).
        """
        # Simulate an API error response
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.json.return_value = {}

        mock_get.return_value = mock_response

        # Call the function with mock data
        df = create_daily_stock_prices_table('AAPL', '2024-09-10', 'dummy_api_key')

        # The DataFrame should be empty when there's an API error
        self.assertTrue(df.empty)


if __name__ == "__main__":
    unittest.main()
