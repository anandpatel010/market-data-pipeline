import sqlite3

# Connect to SQLite database
DB_FILE = 'financial_data.db'
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

def remove_duplicates(table_name, unique_columns):
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
    print(f"Removed {rows_removed} duplicates from {table_name}.")
    
    return rows_removed

# Remove duplicates from stock_data table
stock_removed = remove_duplicates('stock_data', ['timestamp', 'symbol'])

# Remove duplicates from crypto_data table
crypto_removed = remove_duplicates('crypto_data', ['timestamp', 'symbol'])

# Commit the changes and close the connection
conn.commit()
conn.close()

# Print total duplicates removed
total_removed = stock_removed + crypto_removed
print(f"Total duplicates removed: {total_removed}")
