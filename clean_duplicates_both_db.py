import sqlite3

def remove_duplicates(db_file, table_name, unique_columns):
    # Connect to SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Find duplicates based on unique columns
    query = f"""
        DELETE FROM {table_name}
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM {table_name}
            GROUP BY {', '.join(unique_columns)}
        )
    """
    # Execute the query
    cursor.execute(query)
    
    # Count how many rows were affected (i.e., how many duplicates were removed)
    rows_removed = cursor.rowcount
    print(f"Removed {rows_removed} duplicates from {table_name} in {db_file}.")
    
    # Commit the changes and close the connection
    conn.commit()
    conn.close()
    
    return rows_removed

# Database files
DAILY_DB_FILE = 'financial_data_daily.db'
MIN_24HR_DB_FILE = 'financial_data_1min_24hrs.db'

# Remove duplicates from daily data tables
daily_stock_removed = remove_duplicates(DAILY_DB_FILE, 'daily_stock_data', ['timestamp', 'symbol'])
daily_crypto_removed = remove_duplicates(DAILY_DB_FILE, 'daily_crypto_data', ['timestamp', 'symbol'])

# Remove duplicates from 1-minute data tables
min_stock_removed = remove_duplicates(MIN_24HR_DB_FILE, 'stock_data', ['timestamp', 'symbol'])
min_crypto_removed = remove_duplicates(MIN_24HR_DB_FILE, 'crypto_data', ['timestamp', 'symbol'])

# Print total duplicates removed
total_removed = daily_stock_removed + daily_crypto_removed + min_stock_removed + min_crypto_removed
print(f"Total duplicates removed from both databases: {total_removed}")
