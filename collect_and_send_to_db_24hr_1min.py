import requests
import pandas as pd
import sqlite3
import time
from pycoingecko import CoinGeckoAPI
import io

# Configuration
API_KEY = '4EYAGQ5C5GQ37B1C'
DB_FILE = 'financial_data_1min_24hrs.db'  # SQLite database file

# Example lists
nasdaq_stocks = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA', 'META', 'AMZN']  # Stock symbols
top_10_cryptos = ['bitcoin', 'ethereum', 'binancecoin', 'ripple', 'cardano', 'solana', 'dogecoin', 'polkadot', 'matic-network', 'litecoin']  # Crypto symbols

# Connect to SQLite database
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Initialize CoinGecko API client
cg = CoinGeckoAPI()

# Function to create tables if they don't exist
def create_tables():
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_data (
            timestamp TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            symbol TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS crypto_data (
            timestamp TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            symbol TEXT
        )
    ''')
    conn.commit()

# Function to fetch intraday stock data
def fetch_stock_data(symbol):
    print(f"Fetching stock data for {symbol}")
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={API_KEY}&datatype=csv'
    response = requests.get(url)
    
    # Check for API errors or rate limits
    if response.status_code != 200 or "Error Message" in response.text:
        print(f"Error fetching data for {symbol}: {response.text}")
        return pd.DataFrame()  # Return empty DataFrame on error

    data = response.content.decode('utf-8')
    df = pd.read_csv(io.StringIO(data))
    
    # Ensure the first column is 'timestamp'
    if df.columns[0] != 'timestamp':
        df.rename(columns={df.columns[0]: 'timestamp'}, inplace=True)
    
    # Check if required columns are present
    required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        print(f"Warning: Missing columns {missing_columns} in data for {symbol}. Skipping this symbol.")
        return pd.DataFrame()  # Return empty DataFrame if columns are missing

    # Add symbol column and filter required columns
    df['symbol'] = symbol
    df = df[required_columns + ['symbol']]  # Ensure the DataFrame only contains the required columns
    
    return df

# Function to fetch intraday crypto data using CoinGecko
def fetch_crypto_data(symbol):
    print(f"Fetching crypto data for {symbol} from CoinGecko")
    data = cg.get_coin_market_chart_by_id(id=symbol, vs_currency='usd', days='1')
    
    # Convert to DataFrame
    prices = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
    total_volumes = pd.DataFrame(data['total_volumes'], columns=['timestamp', 'volume'])
    
    # Merge data
    df = pd.merge(prices, total_volumes, on='timestamp')
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    
    # Calculate open, high, low, close from prices
    df['open'] = df['price']
    df['high'] = df['price']
    df['low'] = df['price']
    df['close'] = df['price']
    
    # Select relevant columns and add symbol
    df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
    df['symbol'] = symbol
    
    return df

# Function to save stock data to the database
def save_stock_data(df):
    if not df.empty:
        df.to_sql('stock_data', conn, if_exists='append', index=False)
        print(f"Saved stock data for {df['symbol'][0]} to database")
    else:
        print("No data to save for stock.")

# Function to save crypto data to the database
def save_crypto_data(df):
    if not df.empty:
        df.to_sql('crypto_data', conn, if_exists='append', index=False)
        print(f"Saved crypto data for {df['symbol'][0]} to database")
    else:
        print("No data to save for crypto.")

# Main execution
if __name__ == "__main__":
    create_tables()

    # Fetch and save data for all NASDAQ stocks
    for stock in nasdaq_stocks:
        df = fetch_stock_data(stock)
        save_stock_data(df)
        time.sleep(12)  # Sleep to respect API limits

    # Fetch and save data for top 10 cryptocurrencies
    for crypto in top_10_cryptos:
        df = fetch_crypto_data(crypto)
        save_crypto_data(df)
        time.sleep(12)  # Respect API limits

    # Close the database connection
    conn.close()

    print("Data fetching and storing complete!")
