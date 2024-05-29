import mysql.connector

def execute_db_query(query, params=None, commit=False):
    db_config = {
        'user': 'user',
        'password': 'pass',
        'host': 'localhost',
        'database': 'db',
    }
    result = None
    conn = None
    cursor = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(query, params)
        if not commit:
            result = cursor.fetchall()
        if commit:
            conn.commit()
    except mysql.connector.Error as e:
        print(f"SQL Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    return result

def create_exclusion_lists():
    # Split the multi-statement queries into separate calls

    # Delete existing tables if they exist
    delete_table_sp400_query = "DROP TABLE IF EXISTS exclusion_list_sp400;"
    delete_table_query = "DROP TABLE IF EXISTS exclusion_list;"
    execute_db_query(delete_table_sp400_query, commit=True)
    execute_db_query(delete_table_query, commit=True)

    # Create new tables
    create_table_sp400_query = """
    CREATE TABLE exclusion_list_sp400 (
        symbol VARCHAR(10) NOT NULL PRIMARY KEY
    );"""
    create_table_query = """
    CREATE TABLE exclusion_list (
        symbol VARCHAR(10) NOT NULL PRIMARY KEY
    );"""
    execute_db_query(create_table_sp400_query, commit=True)
    execute_db_query(create_table_query, commit=True)

    # Define an array for special symbols
    special_symbols = ['BTCUSD', 'BKNG']  # Add more symbols to this array as needed

    # Fetch and insert the top 12 stocks plus special symbols into exclusion_list_sp400
    top_12_stocks = fetch_top_stocks(n=25)
    symbols_for_exclusion_sp400 = top_12_stocks + special_symbols
    insert_into_exclusion_list_sp400_query = "INSERT INTO exclusion_list_sp400 (symbol) VALUES (%s);"
    for stock in symbols_for_exclusion_sp400:
        execute_db_query(insert_into_exclusion_list_sp400_query, params=(stock,), commit=True)

    # Fetch and insert the top 5 stocks from stock_scores_sp400 plus special symbols into exclusion_list
    top_5_stocks_sp400 = fetch_top_stocks_sp400(n=5)
    symbols_for_exclusion = top_5_stocks_sp400 + special_symbols
    insert_into_exclusion_list_query = "INSERT INTO exclusion_list (symbol) VALUES (%s);"
    for stock in symbols_for_exclusion:
        execute_db_query(insert_into_exclusion_list_query, params=(stock,), commit=True)

def fetch_top_stocks(n=12):
    """Fetch top n stocks based on core_score from the stock_scores table."""
    query = 'SELECT symbol FROM stock_scores ORDER BY core_score DESC LIMIT %s'
    top_stocks = execute_db_query(query, params=(n,), commit=False)
    return [stock[0] for stock in top_stocks]

def fetch_top_stocks_sp400(n=5):
    """Fetch top n stocks based on core_score from the stock_scores_sp400 table."""
    query = 'SELECT symbol FROM stock_scores_sp400 ORDER BY core_score DESC LIMIT %s'
    top_stocks_sp400 = execute_db_query(query, params=(n,), commit=False)
    return [stock[0] for stock in top_stocks_sp400]

if __name__ == "__main__":
    create_exclusion_lists()