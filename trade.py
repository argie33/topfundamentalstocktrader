import mysql.connector
import alpaca_trade_api as tradeapi
from datetime import datetime, timedelta
import logging
import backoff
from requests.exceptions import HTTPError, ConnectionError
import threading
import time

# Setup structured logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
API_KEY = 'yourapikey'
API_SECRET = 'yourapisecret'
BASE_URL = 'https://api.alpaca.markets'
DATA_URL = 'https://data.alpaca.markets'
DB_NAME = 'dbname'
DB_USER = 'user'
DB_PASS = 'pass'
DB_HOST = 'localhost'
DB_PORT = 'port'

# Initialize Alpaca API
api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version='v2')
data_api = tradeapi.REST(API_KEY, API_SECRET, DATA_URL, api_version='v2')  # Initialize data API

# Exponential backoff settings
max_tries = 5  # Maximum number of retry attempts
base_backoff = 1  # Base backoff duration in seconds

def get_db_connection():
    """Establishes a database connection."""
    return mysql.connector.connect(user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME, port=DB_PORT)

@backoff.on_exception(backoff.expo, mysql.connector.Error, max_tries=max_tries, base=base_backoff)
def execute_db_query(query, params=None, commit=False):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(buffered=True)
        cursor.execute(query, params or ())
        if commit:
            conn.commit()
            logging.info("Transaction committed successfully.")
        else:
            results = cursor.fetchall()
            logging.info(f"Fetched {len(results)} records.")
            return results
    except mysql.connector.Error as e:
        logging.error(f"Database error: {e}")
        if conn:
            conn.rollback()
            logging.info("Transaction rolled back due to an error.")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def robust_api_call(api_function, *args, **kwargs):
    """Executes an API function with retry logic."""
    return api_function(*args, **kwargs)

def correct_symbol_format(symbol):
    """Corrects the stock symbol format for compatibility with the API."""
    return symbol.replace('-', '.')

def format_symbol_for_db(symbol):
    """Converts stock symbols from dot notation (API format) to dash notation (database format)."""
    return symbol.replace('.', '-')

# Hardcoded retention list of stock symbols
retention_list = ["NVDA", "BRK.B", "AMZN", "NVR", "AAPL", "MSFT", "META", "GOOGL", "DE"]

def fetch_wash_sale_symbols():
    """Fetches symbols from the wash_sale table."""
    wash_sale_query = "SELECT symbol FROM wash_sale"
    wash_sale_symbols = execute_db_query(wash_sale_query)
    return {format_symbol_for_db(symbol[0]) for symbol in wash_sale_symbols}

def fetch_top_stocks(n=10):
    """
    Fetch top n stocks based on core_score from the database, ensuring that stocks in the retention list and not in wash sales are included.
    Returns data including core_score, quality_score, and growth_score for further processing.
    """
    current_positions = get_current_positions()
    wash_sale_symbols = fetch_wash_sale_symbols()

    # Fetch all stock scores
    query = "SELECT symbol, core_score, quality_score, growth_score FROM stock_screener_scores"
    all_stocks = execute_db_query(query)
    
    # Maps for quick lookup
    stocks_map = {correct_symbol_format(stock[0]): (stock[1], stock[2], stock[3]) for stock in all_stocks}
    existing_positions_set = set(current_positions.keys())  # Adjusted to use dictionary keys

    # Prepare lists
    final_stocks = []

    # Add retention stocks first
    for symbol in retention_list:
        if symbol in stocks_map:
            final_stocks.append((symbol, *stocks_map[symbol]))

    # If retention list already meets or exceeds n, truncate the list
    if len(final_stocks) >= n:
        return final_stocks[:n]

    # Process remaining stocks to find the top candidates
    for symbol, (core_score, quality_score, growth_score) in stocks_map.items():
        if symbol not in retention_list and symbol not in existing_positions_set and quality_score > 0 and growth_score > 0 and symbol not in wash_sale_symbols:
            final_stocks.append((symbol, core_score, quality_score, growth_score))
        elif symbol in existing_positions_set and symbol not in retention_list:
            final_stocks.append((symbol, core_score, quality_score, growth_score))

    # Sort final stocks by core_score in descending order
    final_stocks.sort(key=lambda x: x[1], reverse=True)

    # Limit to the top n stocks
    return final_stocks[:n]

def get_current_positions():
    """Fetches current positions from the Alpaca account."""
    positions = robust_api_call(api.list_positions)
    return {position.symbol: position for position in positions}

def attempt_order(symbol, side, notional=None, quantity=None, limit_price=None, trail_percent=None):
    symbol = correct_symbol_format(symbol)

    # Define order parameters
    order_params = {
        "symbol": symbol,
        "side": side,
        "type": "market",
        "time_in_force": "day"
    }

    if limit_price is not None:
        order_params["type"] = "limit"
        order_params["limit_price"] = round(limit_price, 2)
        order_params["qty"] = int(quantity)  # Ensure quantity is an integer
    elif trail_percent is not None:
        order_params["type"] = "trailing_stop"
        order_params["trail_percent"] = round(trail_percent, 2)
        order_params["qty"] = int(quantity)
    elif quantity is not None:
        order_params["qty"] = int(quantity)
    elif notional is not None:
        order_params["notional"] = round(notional, 2)  # Adjusting precision for notional orders

    logging.debug(f"Submitting order: {order_params}")

    # Execute the order through Alpaca API with retry logic
    try:
        order = robust_api_call(api.submit_order, **order_params)
        logging.info(f"Order for {symbol} ({side}) submitted successfully. Order ID: {order.id}, " +
                     (f"Quantity: {quantity}" if quantity else f"Notional: {notional}" if notional else f"Limit price: {limit_price}"))
    except Exception as e:
        logging.error(f"Failed to submit order for {symbol} due to: {e}")

def fetch_exclusion_list():
    """
    Fetches the list of symbols to be excluded from the investment calculation from the 'exclusion_list_sp400' table in the database.
    Returns a list of symbols, converted to the API-compatible format.
    """
    exclusion_list_query = "SELECT symbol FROM exclusion_list"
    exclusion_list = execute_db_query(exclusion_list_query)
    # Apply format correction for each symbol
    return [correct_symbol_format(symbol[0]) for symbol in exclusion_list]

def fetch_stock_beta(symbol):
    """Fetches the beta value of a stock from the stock_data table."""
    beta_query = "SELECT beta FROM stock_data WHERE symbol = %s"
    result = execute_db_query(beta_query, (format_symbol_for_db(symbol),))
    if result and len(result) > 0:
        return float(result[0][0])
    return 1  # Default beta if not found

def calculate_trailing_stop_loss_percentage(gain_percentage, beta):
    """Calculates the trailing stop loss percentage based on gain and beta."""
    base_trailing_stop = 1  # Minimum trailing stop loss percentage
    gain_factor = 0.1  # Adjust the gain factor as needed
    beta_factor = 0.5  # Adjust the beta factor as needed
    adjusted_stop = base_trailing_stop + (gain_percentage * gain_factor) + (beta * beta_factor)
    return min(adjusted_stop, 20)  # Cap the trailing stop at a maximum of 20%

def verify_top_stocks_scores_exist(top_stocks):
    """
    Verifies that each of the top stocks has an entry in the stock_scores table.
    If any of the scores are missing, the function returns False.

    Args:
        top_stocks (list of str): The symbols of the top stocks to check in API format (e.g., BRK.B).

    Returns:
        bool: True if all top stocks have an entry in the stock_scores table, False otherwise.
    """
    missing_scores = False
    conn = get_db_connection()
    cursor = conn.cursor()

    for stock in top_stocks:
        stock_db_format = format_symbol_for_db(stock)  # Convert to database format
        query = "SELECT COUNT(*) FROM stock_scores WHERE symbol = %s"
        cursor.execute(query, (stock_db_format,))
        result = cursor.fetchone()
        if result[0] == 0:  # If no entry exists for the stock in the database format
            logging.error(f"No entry exists in stock_scores for {stock} (database format: {stock_db_format}).")
            missing_scores = True

    cursor.close()
    conn.close()

    return not missing_scores

def swap_stocks():
    logging.info("Fetching and adjusting top stocks with retention list included.")
    top_stocks = fetch_top_stocks()  # Fetch and adjust top stocks considering the retention list
    
    # Ensure correct symbol format, now extracting symbol from tuple
    top_stocks_formatted = [correct_symbol_format(stock[0]) for stock in top_stocks]  
    
    # Verify that all top stocks have an entry in the stock_screener_scores table
    if not verify_top_stocks_scores_exist(top_stocks_formatted):
        logging.error("Data integrity issue detected: Missing scores for top stocks. Exiting program.")
        return  # Exit the function if any top stocks are missing scores

    current_positions = get_current_positions()  # Get current portfolio positions
    total_account_value = float(robust_api_call(api.get_account).portfolio_value)
    available_cash = float(robust_api_call(api.get_account).cash)  # Fetch available cash
    target_investment_value = total_account_value * 0.812  # Adjust according to your strategy

    exclusion_list = fetch_exclusion_list()  # Fetch stocks to be excluded from the trading calculations

    # Calculate the target investment per stock
    target_investment_per_stock = target_investment_value / len(top_stocks_formatted)

    logging.info("Initiating sell orders for stocks not in the top list and not on the exclusion list.")
    for position_symbol, position in current_positions.items():
        if position_symbol not in top_stocks_formatted and position_symbol not in exclusion_list and position_symbol not in retention_list:
            logging.info(f"Selling entire position of {position_symbol}, quantity: {position.qty}, to reallocate funds.")
            quantity = float(position.qty)
            current_price = float(position.current_price)
            market_value = float(position.market_value)
            avg_entry_price = float(position.avg_entry_price)

            if quantity < 1:
                # Sell fractional shares as notional
                notional_value = round(quantity * current_price * 0.99, 2)  # Apply a small buffer to avoid precision issues
                attempt_order(position_symbol, 'sell', notional=notional_value)
            else:
                gain_percentage = ((current_price - avg_entry_price) / avg_entry_price) * 100
                beta = fetch_stock_beta(position_symbol)
                trailing_stop_percentage = calculate_trailing_stop_loss_percentage(gain_percentage, beta)
                
                if gain_percentage < 2:  # Update to 2% threshold
                    # If the gain is too small, sell immediately
                    logging.info(f"Gain for {position_symbol} is less than 2%. Selling immediately.")
                    limit_price = round(current_price * 0.97, 2)
                    full_shares = int(quantity)
                    fractional_shares = quantity - full_shares

                    if full_shares > 0:
                        attempt_order(position_symbol, 'sell', quantity=full_shares, limit_price=limit_price)
                    if fractional_shares > 0:
                        notional_value = round(fractional_shares * current_price * 0.99, 2)  # Apply a small buffer to avoid precision issues
                        attempt_order(position_symbol, 'sell', notional=notional_value)
                else:
                    logging.info(f"Placing trailing stop order for {position_symbol} with a stop of {trailing_stop_percentage}%.")
                    full_shares = int(quantity)
                    fractional_shares = quantity - full_shares

                    if full_shares > 0:
                        attempt_order(position_symbol, 'sell', quantity=full_shares, trail_percent=trailing_stop_percentage)
                    if fractional_shares > 0:
                        notional_value = round(fractional_shares * current_price * 0.99, 2)  # Apply a small buffer to avoid precision issues
                        attempt_order(position_symbol, 'sell', notional=notional_value)

    logging.info("Waiting for sell orders to complete before proceeding with buy orders.")
    ensure_all_sells_complete_before_buy()

    logging.info("Proceeding with buy orders for top stocks.")
    for stock_tuple in top_stocks:
        stock_symbol = correct_symbol_format(stock_tuple[0])
        core_score, quality_score, growth_score = stock_tuple[1], stock_tuple[2], stock_tuple[3]

        # Check if the stock meets the criteria for buying
        if stock_symbol not in current_positions and quality_score > 0 and growth_score > 0:
            logging.info(f"Buying {stock_symbol} to add to the portfolio, targeting investment of {target_investment_per_stock}.")
            buy_shares(stock_symbol, target_investment_per_stock)
        elif stock_symbol in current_positions:
            current_investment = float(current_positions[stock_symbol].market_value)
            if current_investment < target_investment_per_stock:
                additional_investment_needed = target_investment_per_stock - current_investment
                logging.info(f"Adjusting position for {stock_symbol}. Current investment: {current_investment}. Buying additional amount to reach target investment of {target_investment_per_stock}.")
                buy_shares(stock_symbol, additional_investment_needed)

def buy_shares(symbol, investment_value):
    """Buys shares for a given symbol with a target investment value."""
    current_price = float(robust_api_call(data_api.get_latest_trade, symbol).price)
    full_shares = int(investment_value // current_price)
    fractional_value = investment_value - (full_shares * current_price)

    if full_shares > 0:
        limit_price = round(current_price * 1.03, 2)  # 3% buffer for buy limit orders
        attempt_order(symbol, 'buy', quantity=full_shares, limit_price=limit_price)
    if fractional_value > 0:
        notional_value = round(fractional_value, 2)
        attempt_order(symbol, 'buy', notional=notional_value)

def ensure_all_sells_complete_before_buy():
    """
    Ensures all sell orders are completed (either filled or canceled) before proceeding.
    """
    logging.info("Ensuring all sell orders are completed before proceeding with buy orders.")
    
    # Retrieve a list of all sell order IDs that need to be checked.
    # This example assumes you have a way to retrieve these IDs from your database or memory.
    sell_orders = execute_db_query("SELECT order_id FROM orders WHERE order_type = 'sell' AND status NOT IN ('filled', 'canceled')")
    sell_order_ids = [order[0] for order in sell_orders]  # Extracting order IDs from the query result

    all_sells_completed = False
    while not all_sells_completed:
        all_sells_completed = True  # Assume all sells are completed initially
        for order_id in sell_order_ids:
            order = robust_api_call(api.get_order, order_id)  # Use robust_api_call or directly api.get_order
            if order.status not in ['filled', 'canceled']:
                all_sells_completed = False
                logging.info(f"Waiting for sell order {order_id} to complete. Current status: {order.status}")
                break  # Exit the loop early if any sell order is not completed
        
        if not all_sells_completed:
            time.sleep(5)  # Wait for 30 seconds before checking again
        else:
            logging.info("All sell orders completed.")

def main():
    logging.info("Starting the trading script.")
    logging.info("Check if any adjustments are needed (swap stocks).")    
    swap_stocks()

if __name__ == '__main__':
    main()
