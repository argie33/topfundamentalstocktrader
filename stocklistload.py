import mysql.connector
import pandas as pd

# Replace with your MySQL database credentials
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'user',
    'password': 'pass',
    'database': 'db'
}

def create_stock_symbols_table():
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = connection.cursor()

    # Drop the existing 'stock_symbols' table if it exists
    cursor.execute("DROP TABLE IF EXISTS stock_symbols")
    connection.commit()

    # Create stock_symbols table if it doesn't exist
    create_table_query = """
        CREATE TABLE IF NOT EXISTS stock_symbols (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(10) UNIQUE NOT NULL,
            sector VARCHAR(100) NOT NULL,
            index_name VARCHAR(10) NOT NULL
        );
    """
    cursor.execute(create_table_query)
    connection.commit()

    cursor.close()
    connection.close()

def load_stock_symbols(stock_symbols, index_name):
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = connection.cursor()

    # Insert or update stock symbols in the table
    insert_query = """
        INSERT INTO stock_symbols (symbol, sector, index_name) VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE index_name = CASE 
            WHEN index_name = 'NASDAQ100' AND %s LIKE 'S&P%%' THEN VALUES(index_name) 
            ELSE index_name 
        END;
    """
    for symbol, sector in stock_symbols:
        cursor.execute(insert_query, (symbol, sector, index_name, index_name))

    connection.commit()
    cursor.close()
    connection.close()

def fetch_stock_symbols(url, symbol_col, sector_col=None):
    tables = pd.read_html(url)
    for table in tables:
        if symbol_col in table.columns:
            if sector_col and sector_col in table.columns:
                stock_symbols = [(symbol.replace('.', '-'), sector) for symbol, sector in table[[symbol_col, sector_col]].values.tolist()]
            else:
                stock_symbols = [(symbol.replace('.', '-'), 'Unknown') for symbol in table[symbol_col].tolist()]
            return stock_symbols
    return []

create_stock_symbols_table()

# S&P 500
sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
sp500_stock_symbols = fetch_stock_symbols(sp500_url, 'Symbol', 'GICS Sector')
load_stock_symbols(sp500_stock_symbols, "S&P500")
print("S&P 500 stock symbols have been loaded successfully.")

# S&P 600
#sp600_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_600_companies'
#sp600_stock_symbols = fetch_stock_symbols(sp600_url, 'Symbol', 'GICS Sector')
#load_stock_symbols(sp600_stock_symbols, "S&P600")
#print("S&P 600 stock symbols have been loaded successfully.")

# S&P 400
#sp400_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_400_companies'
#sp400_stock_symbols = fetch_stock_symbols(sp400_url, 'Symbol', 'GICS Sector')
#load_stock_symbols(sp400_stock_symbols, "S&P400")
#print("S&P 400 stock symbols have been loaded successfully.")
