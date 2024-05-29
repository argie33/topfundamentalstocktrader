import requests
import time
import mysql.connector
import random
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration constants
FMP_API_KEY = 'yourapikeyhere'
FMP_BASE_URL = 'https://financialmodelingprep.com/api/v3/ratios-ttm'
REQUEST_DELAY = 0.2  # seconds between requests to prevent hitting API rate limits
DAILY_REQUEST_LIMIT = 9999999999  # API limit for FinancialModelingPrep or a safe threshold
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

# Function to create or recreate the stock_ratios table
def create_stock_ratios_table():
    execute_query("DROP TABLE IF EXISTS stock_ratios")
    execute_query("""
        CREATE TABLE stock_ratios (
            symbol VARCHAR(10),
            dividendYielTTM FLOAT,
            dividendYielPercentageTTM FLOAT,
            peRatioTTM FLOAT,
            pegRatioTTM FLOAT,
            payoutRatioTTM FLOAT,
            currentRatioTTM FLOAT,
            quickRatioTTM FLOAT,
            cashRatioTTM FLOAT,
            daysOfSalesOutstandingTTM FLOAT,
            daysOfInventoryOutstandingTTM FLOAT,
            operatingCycleTTM FLOAT,
            daysOfPayablesOutstandingTTM FLOAT,
            cashConversionCycleTTM FLOAT,
            grossProfitMarginTTM FLOAT,
            operatingProfitMarginTTM FLOAT,
            pretaxProfitMarginTTM FLOAT,
            netProfitMarginTTM FLOAT,
            effectiveTaxRateTTM FLOAT,
            returnOnAssetsTTM FLOAT,
            returnOnEquityTTM FLOAT,
            returnOnCapitalEmployedTTM FLOAT,
            netIncomePerEBTTTM FLOAT,
            ebtPerEbitTTM FLOAT,
            ebitPerRevenueTTM FLOAT,
            debtRatioTTM FLOAT,
            debtEquityRatioTTM FLOAT,
            longTermDebtToCapitalizationTTM FLOAT,
            totalDebtToCapitalizationTTM FLOAT,
            interestCoverageTTM FLOAT,
            cashFlowToDebtRatioTTM FLOAT,
            companyEquityMultiplierTTM FLOAT,
            receivablesTurnoverTTM FLOAT,
            payablesTurnoverTTM FLOAT,
            inventoryTurnoverTTM FLOAT,
            fixedAssetTurnoverTTM FLOAT,
            assetTurnoverTTM FLOAT,
            operatingCashFlowPerShareTTM FLOAT,
            freeCashFlowPerShareTTM FLOAT,
            cashPerShareTTM FLOAT,
            operatingCashFlowSalesRatioTTM FLOAT,
            freeCashFlowOperatingCashFlowRatioTTM FLOAT,
            cashFlowCoverageRatiosTTM FLOAT,
            shortTermCoverageRatiosTTM FLOAT,
            capitalExpenditureCoverageRatioTTM FLOAT,
            dividendPaidAndCapexCoverageRatioTTM FLOAT,
            priceBookValueRatioTTM FLOAT,
            priceToBookRatioTTM FLOAT,
            priceToSalesRatioTTM FLOAT,
            priceEarningsRatioTTM FLOAT,
            priceToFreeCashFlowsRatioTTM FLOAT,
            priceToOperatingCashFlowsRatioTTM FLOAT,
            priceCashFlowRatioTTM FLOAT,
            priceEarningsToGrowthRatioTTM FLOAT,
            priceSalesRatioTTM FLOAT,
            enterpriseValueMultipleTTM FLOAT,
            priceFairValueTTM FLOAT,
            dividendPerShareTTM FLOAT,
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
    create_stock_ratios_table()
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
            insert_query = f"INSERT INTO stock_ratios (symbol, {', '.join(fields)}) VALUES ({placeholders})"
            execute_query(insert_query, (symbol,) + tuple(data.values()))
            logging.info(f"Inserted data for {symbol}.")
        time.sleep(REQUEST_DELAY)
        requests_today += 1

if __name__ == "__main__":
    main()
