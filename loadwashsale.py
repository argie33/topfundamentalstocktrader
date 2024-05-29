import alpaca_trade_api as tradeapi
import pandas as pd
import pymysql
from collections import deque
from datetime import datetime, timedelta, timezone
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Alpaca API credentials
API_KEY = 'yourapikey'
API_SECRET = 'yourapisecret'
BASE_URL = 'https://api.alpaca.markets'  # or use the live URL for real transactions

api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version='v2')

# Exclusion list
exclusion_symbols = []  # Add symbols here to exclude them from the wash sale table

def initialize_db():
    try:
        conn = pymysql.connect(
            host='localhost',  # replace with your host, e.g., 'localhost'
            user='user',       # replace with your username, usually 'root'
            password='pass',       # replace with your password
            db='db',       # replace with your database name
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS wash_sale")
            cursor.execute("""
                CREATE TABLE wash_sale (
                    symbol VARCHAR(255) UNIQUE
                )
            """)
            conn.commit()
        logging.info("Database initialized and wash_sale table created.")
        return conn
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}")
        return None

def fetch_all_orders(api):
    all_orders = []
    current_time = datetime.now(timezone.utc)
    start_time = current_time - timedelta(days=365 * 5)
    last_timestamp = None

    try:
        while True:
            after_param = last_timestamp.isoformat() if last_timestamp else start_time.isoformat()
            orders = api.list_orders(
                status='all',
                limit=500,
                after=after_param,
                direction='asc'
            )
            if orders:
                all_orders.extend(orders)
                last_order = orders[-1]
                last_timestamp = last_order.submitted_at
            if not orders or len(orders) < 500:
                break
        logging.info(f"Total orders fetched: {len(all_orders)}")
    except Exception as e:
        logging.error(f"Failed to fetch orders: {e}")
    return all_orders

def calculate_fifo_gains(api, conn):
    orders = fetch_all_orders(api)
    buys = {}
    sells = []
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    wash_sale_symbols = set()

    if not orders:
        logging.warning("No orders retrieved. Please check API settings and connectivity.")
        return pd.DataFrame()

    try:
        with conn.cursor() as cursor:
            for order in orders:
                symbol = order.symbol
                if symbol in exclusion_symbols:
                    continue
                if order.side == 'buy' and order.status == 'filled' and order.filled_qty:
                    buy_price = float(order.filled_avg_price)
                    buy_qty = float(order.filled_qty)
                    if symbol not in buys:
                        buys[symbol] = deque()
                    buys[symbol].append((buy_price, buy_qty))
                elif order.side == 'sell' and order.status == 'filled' and order.filled_qty:
                    if symbol in buys and buys[symbol]:
                        sell_price = float(order.filled_avg_price)
                        sell_qty = float(order.filled_qty)
                        cost_basis = 0
                        remaining_qty = sell_qty
                        while remaining_qty > 0 and buys[symbol]:
                            buy_price, buy_qty = buys[symbol].popleft()
                            used_qty = min(buy_qty, remaining_qty)
                            cost_basis += used_qty * buy_price
                            remaining_qty -= used_qty
                            if buy_qty > used_qty:
                                buys[symbol].appendleft((buy_price, buy_qty - used_qty))
                        gain_loss = (sell_price - (cost_basis / sell_qty)) * sell_qty
                        sells.append({
                            'Symbol': symbol,
                            'Quantity Sold': sell_qty,
                            'Sell Price': sell_price,
                            'Cost Basis per Share': cost_basis / sell_qty,
                            'Gain/Loss': gain_loss,
                            'Sell Date': order.created_at
                        })
                        if gain_loss < 0 and order.created_at >= thirty_days_ago and symbol not in wash_sale_symbols:
                            cursor.execute("INSERT IGNORE INTO wash_sale (symbol) VALUES (%s)", (symbol,))
                            conn.commit()
                            wash_sale_symbols.add(symbol)
                            logging.info(f"Record inserted for {symbol}")
        return pd.DataFrame(sells)
    except Exception as e:
        logging.error(f"Failed during FIFO gain calculations or database operations: {e}")
        return pd.DataFrame()

try:
    conn = initialize_db()
    if conn:
        sales_report = calculate_fifo_gains(api, conn)
        if not sales_report.empty:
            print(sales_report)
        else:
            logging.info("No sales transactions were processed.")
finally:
    if conn:
        conn.close()
        logging.info("Database connection closed.")
