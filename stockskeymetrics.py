import requests
import time
import mysql.connector
import random
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration constants
FMP_API_KEY = 'yourapikeyhere'
FMP_BASE_URL = 'https://financialmodelingprep.com/api/v3/key-metrics-ttm'
REQUEST_DELAY = 0.2  # seconds between requests to prevent hitting API rate limits
DAILY_REQUEST_LIMIT = 99999999  # API limit for FinancialModelingPrep or a safe threshold
RETRY_LIMIT = 5  # Maximum number of retries for API requests
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'user',
    'password': 'pass',
    'database': 'db'
}

# Function to execute SQL queries
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

# Function to create or recreate the stock_key_metrics table
def create_stock_key_metrics_table():
    execute_query("DROP TABLE IF EXISTS stock_key_metrics")
    execute_query("""
        CREATE TABLE stock_key_metrics (
            symbol VARCHAR(10),
            revenuePerShareTTM FLOAT,
            netIncomePerShareTTM FLOAT,
            operatingCashFlowPerShareTTM FLOAT,
            freeCashFlowPerShareTTM FLOAT,
            cashPerShareTTM FLOAT,
            bookValuePerShareTTM FLOAT,
            tangibleBookValuePerShareTTM FLOAT,
            shareholdersEquityPerShareTTM FLOAT,
            interestDebtPerShareTTM FLOAT,
            marketCapTTM BIGINT,
            enterpriseValueTTM BIGINT,
            peRatioTTM FLOAT,
            priceToSalesRatioTTM FLOAT,
            pocfratioTTM FLOAT,
            pfcfRatioTTM FLOAT,
            pbRatioTTM FLOAT,
            ptbRatioTTM FLOAT,
            evToSalesTTM FLOAT,
            enterpriseValueOverEBITDATTM FLOAT,
            evToOperatingCashFlowTTM FLOAT,
            evToFreeCashFlowTTM FLOAT,
            earningsYieldTTM FLOAT,
            freeCashFlowYieldTTM FLOAT,
            debtToEquityTTM FLOAT,
            debtToAssetsTTM FLOAT,
            netDebtToEBITDATTM FLOAT,
            currentRatioTTM FLOAT,
            interestCoverageTTM FLOAT,
            incomeQualityTTM FLOAT,
            dividendYieldTTM FLOAT,
            dividendYieldPercentageTTM FLOAT,
            payoutRatioTTM FLOAT,
            salesGeneralAndAdministrativeToRevenueTTM FLOAT,
            researchAndDevelopementToRevenueTTM FLOAT,
            intangiblesToTotalAssetsTTM FLOAT,
            capexToOperatingCashFlowTTM FLOAT,
            capexToRevenueTTM FLOAT,
            capexToDepreciationTTM FLOAT,
            stockBasedCompensationToRevenueTTM FLOAT,
            grahamNumberTTM FLOAT,
            roicTTM FLOAT,
            returnOnTangibleAssetsTTM FLOAT,
            grahamNetNetTTM FLOAT,
            workingCapitalTTM BIGINT,
            tangibleAssetValueTTM BIGINT,
            netCurrentAssetValueTTM BIGINT,
            investedCapitalTTM FLOAT,
            averageReceivablesTTM BIGINT,
            averagePayablesTTM BIGINT,
            averageInventoryTTM BIGINT,
            daysSalesOutstandingTTM FLOAT,
            daysPayablesOutstandingTTM FLOAT,
            daysOfInventoryOnHandTTM FLOAT,
            receivablesTurnoverTTM FLOAT,
            payablesTurnoverTTM FLOAT,
            inventoryTurnoverTTM FLOAT,
            roeTTM FLOAT,
            capexPerShareTTM FLOAT,
            dividendPerShareTTM FLOAT,
            debtToMarketCapTTM FLOAT,
            PRIMARY KEY(symbol)
        )
    """)

# Function to fetch data with retry logic
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

# Main function to process all stock symbols
def main():
    create_stock_key_metrics_table()
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
            num_fields = len(fields) + 1
            placeholders = ', '.join(['%s'] * num_fields)
            insert_query = f"INSERT INTO stock_key_metrics (symbol, {', '.join(fields)}) VALUES ({placeholders})"
            execute_query(insert_query, (symbol,) + tuple(data.values()))
            logging.info(f"Inserted data for {symbol}.")
        time.sleep(REQUEST_DELAY)
        requests_today += 1

if __name__ == "__main__":
    main()
