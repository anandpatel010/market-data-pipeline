import requests
import pandas as pd
import time

API_KEY = '4EYAGQ5C5GQ37B1C'

# Example lists
nasdaq_stocks = ['AAPL', 'MSFT', 'GOOGL']  # You can replace with the full list
top_10_cryptos = ['BTC', 'ETH', 'BNB', 'XRP', 'ADA', 'SOL', 'DOGE', 'DOT', 'MATIC', 'LTC']

# Function to fetch intraday stock data
def fetch_stock_data(symbol):
    print(f"Fetching stock data for {symbol}")
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={API_KEY}&datatype=csv'
    response = requests.get(url)
    data = response.content.decode('utf-8')
    df = pd.read_csv(pd.compat.StringIO(data))
    df['symbol'] = symbol
    return df

# Function to fetch intraday crypto data
def fetch_crypto_data(symbol):
    print(f"Fetching crypto data for {symbol}")
    url = f'https://www.alphavantage.co/query?function=CRYPTO_INTRADAY&symbol={symbol}&market=USD&interval=1min&apikey={API_KEY}&datatype=csv'
    response = requests.get(url)
    data = response.content.decode('utf-8')
    df = pd.read_csv(pd.compat.StringIO(data))
    df['symbol'] = symbol
    return df

# Fetch data for all NASDAQ stocks
all_stock_data = []
for stock in nasdaq_stocks:
    df = fetch_stock_data(stock)
    all_stock_data.append(df)
    time.sleep(12)  # Sleep to respect API limits

# Fetch data for top 10 cryptocurrencies
all_crypto_data = []
for crypto in top_10_cryptos:
    df = fetch_crypto_data(crypto)
    all_crypto_data.append(df)
    time.sleep(12)  # Sleep to respect API limits

# Combine all data into single DataFrames
stock_data_combined = pd.concat(all_stock_data, ignore_index=True)
crypto_data_combined = pd.concat(all_crypto_data, ignore_index=True)

# Save to CSV files
stock_data_combined.to_csv('nasdaq_stocks_intraday.csv', index=False)
crypto_data_combined.to_csv('top_10_cryptos_intraday.csv', index=False)

print("Data fetching complete!")
