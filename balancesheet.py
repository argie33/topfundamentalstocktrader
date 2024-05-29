import requests
import time
import mysql.connector
import random
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration constants
FMP_API_KEY = 'yourapikeyhere'
FMP_BASE_URL = 'https://financialmodelingprep.com/api/v3/balance-sheet-statement'
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

def create_balance_sheet_table():
    execute_query("DROP TABLE IF EXISTS balance_sheets")
    execute_query("""
        CREATE TABLE balance_sheets (
            date DATE,
            symbol VARCHAR(10),
            reportedCurrency VARCHAR(10),
            cik VARCHAR(20),
            fillingDate DATE,
            acceptedDate DATETIME,
            calendarYear INT,
            period VARCHAR(10),
            cashAndCashEquivalents BIGINT,
            shortTermInvestments BIGINT,
            cashAndShortTermInvestments BIGINT,
            netReceivables BIGINT,
            inventory BIGINT,
            otherCurrentAssets BIGINT,
            totalCurrentAssets BIGINT,
            propertyPlantEquipmentNet BIGINT,
            goodwill BIGINT,
            intangibleAssets BIGINT,
            goodwillAndIntangibleAssets BIGINT,
            longTermInvestments BIGINT,
            taxAssets BIGINT,
            otherNonCurrentAssets BIGINT,
            totalNonCurrentAssets BIGINT,
            otherAssets BIGINT,
            totalAssets BIGINT,
            accountPayables BIGINT,
            shortTermDebt BIGINT,
            taxPayables BIGINT,
            deferredRevenue BIGINT,
            otherCurrentLiabilities BIGINT,
            totalCurrentLiabilities BIGINT,
            longTermDebt BIGINT,
            deferredRevenueNonCurrent BIGINT,
            deferredTaxLiabilitiesNonCurrent BIGINT,
            otherNonCurrentLiabilities BIGINT,
            totalNonCurrentLiabilities BIGINT,
            otherLiabilities BIGINT,
            capitalLeaseObligations BIGINT,
            totalLiabilities BIGINT,
            preferredStock BIGINT,
            commonStock BIGINT,
            retainedEarnings BIGINT,
            accumulatedOtherComprehensiveIncomeLoss BIGINT,
            othertotalStockholdersEquity BIGINT,
            totalStockholdersEquity BIGINT,
            totalEquity BIGINT,
            totalLiabilitiesAndStockholdersEquity BIGINT,
            minorityInterest BIGINT,
            totalLiabilitiesAndTotalEquity BIGINT,
            totalInvestments BIGINT,
            totalDebt BIGINT,
            netDebt BIGINT,
            link VARCHAR(255),
            finalLink VARCHAR(255),
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
    create_balance_sheet_table()
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
            fields = data.keys()
            num_fields = len(fields)
            placeholders = ', '.join(['%s'] * num_fields)
            insert_query = f"INSERT INTO balance_sheets ({', '.join(fields)}) VALUES ({placeholders})"
            execute_query(insert_query, tuple(data.values()))
            logging.info(f"Inserted data for {symbol}.")
        time.sleep(REQUEST_DELAY)
        requests_today += 1

if __name__ == "__main__":
    main()
