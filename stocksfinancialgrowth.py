import requests
import time
import mysql.connector
import random
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration constants
FMP_API_KEY = 'yourapikeyhere'
FMP_BASE_URL = 'https://financialmodelingprep.com/api/v3/financial-growth'
REQUEST_DELAY = 0.2  # seconds between requests to prevent hitting API rate limits
DAILY_REQUEST_LIMIT = 9999999  # API limit for FinancialModelingPrep or a safe threshold
RETRY_LIMIT = 5  # Maximum number of retries for API requests
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'user',
    'password': 'pass',
    'database': 'db'
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

def create_financial_growth_table():
    execute_query("DROP TABLE IF EXISTS financial_growth")
    execute_query("""
        CREATE TABLE financial_growth (
            symbol VARCHAR(10),
            date DATE,
            period VARCHAR(10),
            revenueGrowth FLOAT,
            grossProfitGrowth FLOAT,
            ebitgrowth FLOAT,
            operatingIncomeGrowth FLOAT,
            netIncomeGrowth FLOAT,
            epsgrowth FLOAT,
            epsdilutedGrowth FLOAT,
            weightedAverageSharesGrowth FLOAT,
            weightedAverageSharesDilutedGrowth FLOAT,
            dividendsperShareGrowth FLOAT,
            operatingCashFlowGrowth FLOAT,
            freeCashFlowGrowth FLOAT,
            tenYRevenueGrowthPerShare FLOAT,
            fiveYRevenueGrowthPerShare FLOAT,
            threeYRevenueGrowthPerShare FLOAT,
            tenYOperatingCFGrowthPerShare FLOAT,
            fiveYOperatingCFGrowthPerShare FLOAT,
            threeYOperatingCFGrowthPerShare FLOAT,
            tenYNetIncomeGrowthPerShare FLOAT,
            fiveYNetIncomeGrowthPerShare FLOAT,
            threeYNetIncomeGrowthPerShare FLOAT,
            tenYShareholdersEquityGrowthPerShare FLOAT,
            fiveYShareholdersEquityGrowthPerShare FLOAT,
            threeYShareholdersEquityGrowthPerShare FLOAT,
            tenYDividendperShareGrowthPerShare FLOAT,
            fiveYDividendperShareGrowthPerShare FLOAT,
            threeYDividendperShareGrowthPerShare FLOAT,
            receivablesGrowth FLOAT,
            inventoryGrowth FLOAT,
            assetGrowth FLOAT,
            bookValueperShareGrowth FLOAT,
            debtGrowth FLOAT,
            rdexpenseGrowth FLOAT,
            sgaexpensesGrowth FLOAT,
            calendarYear INT,  # Add this column if it's part of your data
            PRIMARY KEY(symbol, date)
        )
    """)

def fetch_fmp_data(symbol):
    retry_count = 0
    backoff_factor = 0.5
    while retry_count < RETRY_LIMIT:
        try:
            url = f"{FMP_BASE_URL}/{symbol}?period=annual&apikey={FMP_API_KEY}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if data:
                    logging.info(f"Data retrieved successfully for {symbol}.")
                    return data[0]  # Assuming the API returns data as a list of dictionaries
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
    create_financial_growth_table()
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
            fields = [f"`{key}`" for key in data.keys()]  # Use backticks to ensure SQL keywords do not cause issues
            placeholders = ', '.join(['%s'] * len(fields))
            insert_query = f"INSERT INTO financial_growth ({', '.join(fields)}) VALUES ({placeholders})"
            try:
                execute_query(insert_query, tuple(data.values()))
                logging.info(f"Inserted data for {symbol}.")
            except mysql.connector.Error as err:
                logging.error(f"Error inserting data for {symbol}: {err}")
        time.sleep(REQUEST_DELAY)
        requests_today += 1

if __name__ == "__main__":
    main()
