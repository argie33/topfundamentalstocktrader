import requests
import time
import mysql.connector
import random
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration constants
FMP_API_KEY = 'yourapikeyhere'
FMP_BASE_URL = 'https://financialmodelingprep.com/api/v3/profile'
REQUEST_DELAY = 0.2  # seconds between requests to prevent hitting API rate limits
DAILY_REQUEST_LIMIT = 9999999  # API limit for FinancialModelingPrep or a safe threshold
RETRY_LIMIT = 5  # Maximum number of retries for API requests
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'user',
    'password': 'password',
    'database': 'yourdb'
}

def execute_query(query, data=None):
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = connection.cursor()
    try:
        if data is None:
            cursor.execute(query)
        else:
            cursor.execute(query, data)
            connection.commit()
    finally:
        cursor.close()
        connection.close()

def create_company_profiles_table():
    execute_query("DROP TABLE IF EXISTS company_profiles")
    execute_query("""
        CREATE TABLE company_profiles (
            symbol VARCHAR(10),
            price FLOAT,
            beta FLOAT,
            volAvg BIGINT,
            mktCap BIGINT,
            lastDiv FLOAT,
            `range` VARCHAR(255),
            changes FLOAT,
            companyName VARCHAR(255),
            currency VARCHAR(10),
            cik VARCHAR(255),
            isin VARCHAR(255),
            cusip VARCHAR(255),
            exchange VARCHAR(255),
            exchangeShortName VARCHAR(255),
            industry VARCHAR(255),
            website VARCHAR(255),
            description TEXT,
            ceo VARCHAR(255),
            sector VARCHAR(255),
            country VARCHAR(10),
            fullTimeEmployees VARCHAR(255),
            phone VARCHAR(255),
            address VARCHAR(255),
            city VARCHAR(255),
            state VARCHAR(10),
            zip VARCHAR(10),
            dcfDiff FLOAT,
            dcf FLOAT,
            image VARCHAR(255),
            ipoDate DATE,
            defaultImage BOOLEAN,
            isEtf BOOLEAN,
            isActivelyTrading BOOLEAN,
            isAdr BOOLEAN,
            isFund BOOLEAN,
            PRIMARY KEY(symbol)
        )
    """)

def fetch_fmp_data(symbol):
    retry_count = 0
    backoff_factor = 0.5
    while retry_count < RETRY_LIMIT:
        try:
            url = f"{FMP_BASE_URL}/{symbol}?apikey={FMP_API_KEY}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if data:
                    logging.info(f"Data retrieved successfully for {symbol}.")
                    return data[0]
                else:
                    logging.warning(f"No data found for {symbol} on attempt {retry_count + 1}.")
            else:
                logging.error(f"HTTP error {response.status_code} for {symbol} on attempt {retry_count + 1}.")
        except Exception as e:
            logging.error(f"Exception {e} occurred for {symbol} on attempt {retry_count + 1}.")
        
        retry_count += 1
        sleep_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
        logging.info(f"Retrying for {symbol} in {sleep_time:.2f} seconds.")
        time.sleep(sleep_time)

    logging.error(f"All retries exhausted for {symbol}. Giving up.")
    return None

def main():
    create_company_profiles_table()
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = connection.cursor()
    cursor.execute("SELECT symbol FROM stock_symbols")
    symbols = cursor.fetchall()
    cursor.close()
    connection.close()

    requests_today = 0
    for symbol_tuple in symbols:
        symbol = symbol_tuple[0]
        if requests_today >= DAILY_REQUEST_LIMIT:
            logging.info("API request limit reached for today. Stopping further requests.")
            break
        data = fetch_fmp_data(symbol)
        if data:
            # Wrap each key in backticks to handle reserved keywords correctly
            fields = [f"`{key}`" for key in data.keys()]
            placeholders = ', '.join(['%s'] * len(fields))
            insert_query = f"INSERT INTO company_profiles ({', '.join(fields)}) VALUES ({placeholders})"
            try:
                execute_query(insert_query, tuple(data.values()))
                logging.info(f"Inserted data for {symbol}.")
            except mysql.connector.Error as err:
                logging.error(f"Error inserting data for {symbol}: {err}")
        time.sleep(REQUEST_DELAY)
        requests_today += 1

if __name__ == "__main__":
    main()
