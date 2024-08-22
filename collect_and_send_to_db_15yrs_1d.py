import yfinance as yf
import pandas as pd
import sqlite3

# Configuration
DAILY_DB_FILE = 'financial_data_daily.db'  # SQLite database file for daily data
nasdaq_stocks = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA', 'META', 'AMZN']  # Example stock symbols
cryptos = ['BTC-USD', 'ETH-USD', 'BNB-USD', 'XRP-USD', 'ADA-USD', 'SOL-USD', 'DOGE-USD', 'DOT-USD', 'MATIC-USD', 'LTC-USD']

# Connect to SQLite database
conn_daily = sqlite3.connect(DAILY_DB_FILE)
cursor_daily = conn_daily.cursor()

# Function to create tables if they don't exist
def create_daily_tables():
    cursor_daily.execute('''
        CREATE TABLE IF NOT EXISTS daily_stock_data (
            timestamp TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            symbol TEXT
        )
    ''')
    cursor_daily.execute('''
        CREATE TABLE IF NOT EXISTS daily_crypto_data (
            timestamp TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            symbol TEXT
        )
    ''')
    conn_daily.commit()

# Function to fetch daily data using Yahoo Finance
def fetch_daily_data(symbol):
    print(f"Fetching daily data for {symbol}")
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="max")  # Fetch the maximum available data
    df = df.reset_index()

    # Drop unnecessary columns
    df = df.drop(columns=['Dividends', 'Stock Splits'], errors='ignore')

    # Rename columns to match our schema
    df.rename(columns={'Date': 'timestamp', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
    df['symbol'] = symbol

    return df

# Function to save daily data to the database
def save_daily_data(df, table_name):
    if not df.empty:
        df.to_sql(table_name, conn_daily, if_exists='append', index=False)
        print(f"Saved daily data for {df['symbol'][0]} to database")
    else:
        print(f"No data to save for {df['symbol'].iloc[0]}.")

# Main execution
if __name__ == "__main__":
    create_daily_tables()

    # Fetch and save data for all NASDAQ stocks
    for stock in nasdaq_stocks:
        df = fetch_daily_data(stock)
        save_daily_data(df, 'daily_stock_data')

    # Fetch and save data for cryptocurrencies
    for crypto in cryptos:
        df = fetch_daily_data(crypto)
        save_daily_data(df, 'daily_crypto_data')

    # Close the database connection
    conn_daily.close()

    print("Data fetching and storing complete!")

