from datetime import datetime, timedelta
import pytz
import os
from dotenv import load_dotenv
from typing import List, Optional

# Load environment variables from the .env file
DIR_PATH: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path: str = os.path.join(DIR_PATH, '.env')
load_dotenv(dotenv_path)

# Define the timezone for Buenos Aires
BUENOS_AIRES_TZ = pytz.timezone('America/Argentina/Buenos_Aires')

# Get the current date and time in Buenos Aires timezone
NOW_BA: datetime = datetime.now(BUENOS_AIRES_TZ)

# Calculate the date for one days ago
YESTERDAY_DATE: datetime = NOW_BA - timedelta(days=1)

# Format the date as a string in 'YYYY-MM-DD' format
DATE_STR: str = YESTERDAY_DATE.strftime('%Y-%m-%d')

# API keys loaded from environment variables
API_KEY_ALPHA: Optional[str] = os.getenv('API_KEY_ALPHA')
API_KEY_FINHUB: Optional[str] = os.getenv('API_KEY_FINHUB')

# List of stock symbols
STOCKS_SYMBOLS_LIST: List[str] = ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'TSLA']
# You can uncomment the next line to add more symbols to the list
# STOCKS_SYMBOLS_LIST += ['META', 'NVDA', 'MELI', 'JNJ', 'V']

# Redshift database connection details loaded from environment variables
DBNAME_REDSHIFT: Optional[str] = os.getenv('DBNAME_REDSHIFT')
USER_REDSHIFT: Optional[str] = os.getenv('USER_REDSHIFT')
PASSWORD_REDSHIFT: Optional[str] = os.getenv('PASSWORD_REDSHIFT')
HOST_REDSHIFT: Optional[str] = os.getenv('HOST_REDSHIFT')
PORT_REDSHIFT: Optional[str] = os.getenv('PORT_REDSHIFT')

