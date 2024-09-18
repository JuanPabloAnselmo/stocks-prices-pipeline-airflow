import unittest
import os
import pandas as pd
import numpy as np
from typing import Dict
from utils.config import DIR_PATH

# Define the path to the Parquet file
PARQUET_FILE_PATH = os.path.join(DIR_PATH, 'silver', 'data', 'daily_stock_prices_table_silver.parquet')


class TestDataTypesMatch(unittest.TestCase):
    """
    Unit tests to ensure that the data types of the Parquet file
    match the expected data types of the Redshift table.
    """

    def setUp(self) -> None:
        """
        Reads the Parquet file before each test.
        """
        self.df = pd.read_parquet(PARQUET_FILE_PATH)

    def test_column_data_types(self) -> None:
        """
        Test that the data types of the columns in the Parquet file
        match the expected data types of the Redshift table.
        """
        # Define the expected data types according to the Redshift table schema
        expected_dtypes: Dict[str, np.dtype] = {
            'date': np.dtype('object'),         # Expected DATE type (Pandas object type for date strings)
            'symbol': np.dtype('object'),       # Expected TEXT type (Pandas object type for strings)
            'open_price': np.dtype('float64'),  # Expected REAL type (Pandas float64 type)
            'high_price': np.dtype('float64'),  # Expected REAL type (Pandas float64 type)
            'low_price': np.dtype('float64'),   # Expected REAL type (Pandas float64 type)
            'close_price': np.dtype('float64'), # Expected REAL type (Pandas float64 type)
            'volume': np.dtype('float64')       # Expected REAL type (Pandas float64 type)
        }

        # Iterate through each column and verify the data type matches the expected type
        for column, expected_dtype in expected_dtypes.items():
            with self.subTest(column=column):
                actual_dtype = self.df[column].dtype
                self.assertTrue(np.issubdtype(actual_dtype, expected_dtype),
                                f"The data type for column '{column}' is {actual_dtype}, but expected {expected_dtype}.")


if __name__ == "__main__":
    unittest.main()
