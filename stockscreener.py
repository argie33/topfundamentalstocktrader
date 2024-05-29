import pymysql
import numpy as np
from scipy.stats import zscore
from sklearn.linear_model import LinearRegression
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import pandas as pd
import math
from collections import deque
from statistics import stdev
from scipy.signal import argrelextrema

import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def insert_stock_scores(conn, stock_dicts):
    total_stocks = len(stock_dicts)
    stocks_without_scores = []

    for stock in stock_dicts:
        try:
            with conn.cursor() as cursor:
                logging.info(f"Preparing to insert data for stock: {stock['symbol']}")

                query = """
                    INSERT INTO stock_screener_scores (symbol, sector, quality_score, value_score, growth_score, momentum_score, technical_score, baseline_score, core_score, volatility_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                # Check for missing scores and replace with 'N/A'
                values = (
                    stock['symbol'],
                    stock['sector'],
                    stock.get('quality_score', 'N/A'),
                    stock.get('value_score', 'N/A'),
                    stock.get('growth_score', 'N/A'),
                    stock.get('momentum_score', 'N/A'),
                    stock.get('technical_score', 'N/A'),
                    stock.get('volatility_score', 'N/A'),
                    stock.get('baseline_score', 'N/A'),
                    stock.get('core_score', 'N/A'),
                )

                cursor.execute(query, values)
                logging.info(f"Successfully inserted data for stock: {stock['symbol']}")

        except Exception as e:
            logging.error(f"Failed to insert data for stock: {stock['symbol']}. Exception: {e}")

    conn.commit()

    logging.info(f"Data insertion completed. Check your database for records.")

    if stocks_without_scores:
        logging.warning(f"The following stocks were missing some scores: {', '.join(stocks_without_scores)}")

ma_weights = {
    'ma_difference_ratio': 0.25,
    'ma_position': 0.40,
    'ma_distance': 0.25,
    'ma_crossover_status': 0.10
}

def clamp(value, min_value=0, max_value=100):
    return max(min(value, max_value), min_value)

# Database credentials
db_host = 'localhost'
db_user = 'user'
db_password = 'pass'
db_name = 'db'

# Connect to the MySQL database
conn = pymysql.connect(host=db_host, user=db_user, password=db_password, db=db_name)
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS stock_screener_scores")
conn.commit()

cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_screener_scores (
        symbol VARCHAR(255) PRIMARY KEY,
        sector VARCHAR(255),
        quality_score DOUBLE,
        value_score DOUBLE,
        growth_score DOUBLE,
        momentum_score DOUBLE,
        technical_score DOUBLE,
        volatility_score DOUBLE,
        baseline_score DOUBLE,
        core_score DOUBLE
    )
""")
conn.commit()

# Get all unique sectors
cursor.execute("SELECT DISTINCT sector FROM stock_data")
sectors = cursor.fetchall()

excluded_symbols = ["GOOG"]  

for sector in sectors:
    sector = sector[0]

    # If the sector is None, skip it
    if sector is None:
        continue

    print(f"Sector: {sector}")

    # Fetch all stocks in the current sector
   # Get the latest date from the ta table
    cursor.execute("SELECT MAX(date) FROM technical_data")
    latest_date = cursor.fetchone()[0]
   
    # Fetch all stocks in the current sector for the latest date
    cursor.execute("""
        SELECT stock_data.*, technical_data.roc, technical_data.rsi, technical_data.macd, technical_data.macd_signal, technical_data.macd_hist, technical_data.bop, 
        technical_data.bbands_upper, technical_data.bbands_lower, technical_data.bbands_middle, technical_data.close, technical_data.apo, technical_data.ma, technical_data.cmo, technical_data.mom, 
        technical_data.sar, technical_data.willr, technical_data.min, technical_data.max, technical_data.cci, technical_data.aroon_up, technical_data.aroon_down, technical_data.aroonosc, technical_data.ultosc, 
        technical_data.natr, technical_data.atr, technical_data.tsf, technical_data.ad, technical_data.adosc, technical_data.apo, technical_data.tenkan_sen, technical_data.kijun_sen, 
        technical_data.senkou_span_a, technical_data.senkou_span_b, technical_data.chikou_span, technical_data.stdev, technical_data.prev_close, technical_data.sma_10, 
        technical_data.sma_20, technical_data.sma_50, technical_data.sma_200, technical_data.sma_100, technical_data.sma_150, technical_data.volume, technical_data.sma_volume_10, 
        technical_data.bbands_percent_b, technical_data.mfi, technical_data.relative_price_sma_50, technical_data.relative_price_sma_200, 
        technical_data.relative_price_rsi, technical_data.relative_price_bbands_upper, technical_data.relative_price_bbands_lower, 
        technical_data.relative_price_macd_hist, technical_data.relative_close_spy, technical_data.macd_prev, technical_data.macd_hist_prev, 
        technical_data.relative_sector_sma_50, technical_data.relative_sector_sma_200, technical_data.relative_sector_rsi, technical_data.relative_sector_bbands_upper,
        technical_data.relative_sector_bbands_lower, technical_data.relative_sector_close, technical_data.relative_sector_macd_hist
        FROM stock_data
        INNER JOIN technical_data ON stock_data.symbol = technical_data.symbol
        WHERE stock_data.sector = %s AND technical_data.date = %s
    """, (sector, latest_date))
    stocks = cursor.fetchall()

    # Fetch field names
    cursor.execute("DESCRIBE stock_data")
    field_names = [field[0] for field in cursor.fetchall()] + ['roc', 'rsi', 'macd', 'macd_signal', 'macd_hist', 'bop', 'bbands_upper', 'bbands_lower', 'bbands_middle', 'close', 'apo', 'ma', 'cmo', 'mom', 'sar', 'willr', 'min', 'max', 'cci', 'aroon_up', 'aroon_down', 'aroonosc', 'ultosc', 'natr', 'atr', 'tsf', 'ad', 'adosc', 'apo', 'tenkan_sen', 'kijun_sen', 'senkou_span_a', 'senkou_span_b', 'chikou_span', 'stdev', 'prev_close', 'sma_10', 'sma_20', 'sma_50', 'sma_200', 'sma_100', 'sma_150', 'volume', 'sma_volume_10', 'bbands_percent_b', 'mfi', 'relative_price_sma_50', 'relative_price_sma_200', 'relative_price_rsi', 'relative_price_bbands_upper', 'relative_price_bbands_lower', 'relative_price_macd_hist', 'relative_close_spy', 'macd_prev', 'macd_hist_prev', 'relative_sector_sma_50', 'relative_sector_sma_200', 'relative_sector_rsi', 'relative_sector_bbands_upper', 'relative_sector_bbands_lower', 'relative_sector_close', 'relative_sector_macd_hist']

  # Prepare a dictionary for each stock with its field values
    stock_dicts = [dict(zip(field_names, stock)) for stock in stocks]

    for stock in stock_dicts:
        if stock['symbol'] in excluded_symbols:
            continue
        symbol = stock['symbol']

        # Check if necessary fields are in the stock dictionary
        if all(key in stock for key in ['bbands_upper', 'bbands_lower', 'close', 'stdev', 
                                        'sma_50', 'sma_200', 'bbands_percent_b', 'volume', 'rsi', 'sma_volume_10']):
            bollinger_upper_band = stock['bbands_upper']
            bollinger_lower_band = stock['bbands_lower']

            if bollinger_upper_band is not None and bollinger_lower_band is not None:
                bollinger_band_width = bollinger_upper_band - bollinger_lower_band
                bollinger_band_mid = (bollinger_upper_band + bollinger_lower_band) / 2
                standard_deviation = stock['stdev']
                moving_average_50 = stock['sma_50']
                moving_average_200 = stock['sma_200']
                bbands_percent_b = stock['bbands_percent_b']
                volume = stock['volume']
                rsi = stock['rsi']
                sma_volume_10 = stock['sma_volume_10']

                # Price distance from the middle band in terms of standard deviation
                sd_distance = (stock['close'] - bollinger_band_mid) / standard_deviation if standard_deviation != 0 else 0

                # Normalize sd_distance to a score between 0-100 where lower price (more SDs below the mid band) means higher score
                sd_score = max(0, 100 - abs(sd_distance * 10))  # multiplied by 10 for more sensitivity

                # Calculate Bollinger position, normalize it to 0-100 scale where lower price means higher score
                bollinger_position_score = (1 - (stock['close'] - stock['bbands_lower']) / bollinger_band_width if bollinger_band_width != 0 else 0) * 100

                # %B score: values above 1 are considered overbought (hence lower score) and values below 0 are considered oversold (higher score)
                bbands_percent_b_score = max(0, 100 - abs(bbands_percent_b * 100))

                # Reward if both Bollinger Bands are above the 50-day and 200-day Moving Averages, this suggests potential bullish conditions
                if all([band > ma for band in [stock['bbands_lower'], stock['bbands_upper']] for ma in [moving_average_50, moving_average_200]]):
                    reward = 10
                else:
                    reward = 0

                # Calculate the RSI score, values above 70 are overbought (lower score), values below 30 are oversold (higher score)
                rsi_score = max(0, 100 - abs(rsi * 100))

                # Calculate the Relative Volume
                relative_volume = volume / sma_volume_10 if sma_volume_10 != 0 else 0
                # Give a higher score if the volume is higher than the average
                relative_volume_score = relative_volume * 100 if relative_volume <= 1 else 100

                # Combine the scores with equal weights and add the reward
                bollinger_score = ((sd_score + bollinger_position_score + bbands_percent_b_score + rsi_score + relative_volume_score) / 5) + reward

            else:
                bollinger_score = 0

        else:
            # If the required data is not available, set a default score
            bollinger_score = 0

        # Add the calculated Bollinger score to the stock dictionary
        stock['bollinger_score'] = bollinger_score

        # Calculate volatility_score
        bollinger_upper_band = stock['bbands_upper']
        bollinger_lower_band = stock['bbands_lower']
        bollinger_mid = stock['bbands_middle']

        if bollinger_upper_band is not None and bollinger_lower_band is not None and bollinger_mid is not None:
            bollinger_bandwidth = (bollinger_upper_band - bollinger_lower_band) / bollinger_mid

            if bollinger_bandwidth > 0.10:
                bollinger_vol_score = 100  # Extremely high volatility
            elif 0.08 <= bollinger_bandwidth <= 0.10:
                bollinger_vol_score = 75  # High volatility
            elif 0.05 <= bollinger_bandwidth < 0.08:
                bollinger_vol_score = 50  # Medium volatility
            elif 0.02 <= bollinger_bandwidth < 0.05:
                bollinger_vol_score = 25  # Low volatility
            else:  # bollinger_bandwidth < 0.02
                bollinger_vol_score = 0  # Extremely low volatility
        else:
            bollinger_vol_score = 0  # missing Bollinger Bands data
        stock['bollinger_vol_score'] = bollinger_vol_score

#######################################
#    baseline boosters

    quality_metrics = ['returnOnEquityTTM', 'returnOnAssetsTTM', 'returnOnCapitalEmployedTTM', 'operatingProfitMarginTTM', 'grossProfit', 'netProfitMarginTTM', 'revenuePerShareTTM', 'ebitda', 'eps', 'bookValuePerShareTTM', 'Book_Value', 'freeCashFlowYieldTTM', 'roicTTM', 'returnOnCapitalEmployedTTM']
    value_metrics = ['peRatioTTM', 'priceEarningsToGrowthRatioTTM', 'priceToBookRatioTTM', 'priceToSalesRatioTTM', 'enterpriseValueOverEBITDATTM', 'EV_to_Revenue', 'dividendYielTTM', 'priceToFreeCashFlowsRatioTTM', 'pocfratioTTM']
    growth_metrics = ['revenueGrowth', 'epsgrowth', 'freeCashFlowGrowth', 'grossProfitGrowth', 'ebitgrowth', 'dividendsperShareGrowth', 'netIncomeGrowth']
    momentum_metrics = ['roc', 'rsi', 'macd', 'bop', 'apo', 'cmo', 'mom', 'sar', 'willr', 'ultosc']
    technical_metrics = ['bollinger_score']
    volatility_metrics = ['bollinger_vol_score']
    baseline_metrics = ['returnOnEquityTTM',
                        'returnOnAssetsTTM',
                        'operatingProfitMarginTTM',
                        'grossProfit',
                        'grossProfit',
                        'netProfitMarginTTM',
                        'netProfitMarginTTM',
                        'revenuePerShareTTM',
                        'ebitda',
                        'eps',
                        'Book_Value',
                        'bookValuePerShareTTM', 
                        'revenueGrowth',
                        'epsgrowth',
                        'grossProfitGrowth'
                        ]
    core_metrics = ['returnOnEquityTTM', 'returnOnAssetsTTM', 'returnOnCapitalEmployedTTM', 'operatingProfitMarginTTM', 'grossProfit', 'netProfitMarginTTM', 'revenuePerShareTTM', 'ebitda', 'eps', 'bookValuePerShareTTM', 'Book_Value', 'freeCashFlowYieldTTM', 'roicTTM', 'returnOnCapitalEmployedTTM', 
                    'peRatioTTM', 'priceEarningsToGrowthRatioTTM', 'priceToBookRatioTTM', 'priceToSalesRatioTTM', 'enterpriseValueOverEBITDATTM', 'EV_to_Revenue', 'dividendYielTTM', 'priceToFreeCashFlowsRatioTTM', 'pocfratioTTM',
                    'revenueGrowth', 'epsgrowth', 'freeCashFlowGrowth', 'grossProfitGrowth', 'ebitgrowth', 'dividendsperShareGrowth', 'netIncomeGrowth']
    metrics_to_invert = ['peRatioTTM', 'priceToBookRatioTTM', 'priceToSalesRatioTTM', 'enterpriseValueOverEBITDATTM', 'EV_to_Revenue', 'priceToFreeCashFlowsRatioTTM', 'priceEarningsToGrowthRatioTTM', 'pocfratioTTM']

    # Invert the metrics in metrics_to_invert
    for stock in stock_dicts:
        for metric in metrics_to_invert:
            if metric in stock and stock[metric] is not None and stock[metric] != 0:
                stock[metric] = 1 / stock[metric]

    for metric in quality_metrics + value_metrics + growth_metrics + momentum_metrics + baseline_metrics + core_metrics:
        metric_values = [stock[metric] for stock in stock_dicts if stock[metric] is not None]
        z_scores = zscore(metric_values)
        for stock, z_score in zip([s for s in stock_dicts if s[metric] is not None], z_scores):
            stock[f"{metric}_zscore"] = z_score

    # Create factor scores
    print(f"Number of stocks before filtering: {len(stock_dicts)}")
    stock_dicts = [stock for stock in stock_dicts if all(metric in stock for metric in quality_metrics + value_metrics + growth_metrics + momentum_metrics + technical_metrics + baseline_metrics + core_metrics)]
    print(f"Number of stocks after filtering: {len(stock_dicts)}")

    for stock in stock_dicts:

        stock["quality_score"] = np.mean([stock.get(f"{m}_zscore", 0) for m in quality_metrics])
        stock["value_score"] = np.mean([stock.get(f"{m}_zscore", 0) for m in value_metrics])
        stock["growth_score"] = np.mean([stock.get(f"{m}_zscore", 0) for m in growth_metrics])
        stock["momentum_score"] = np.mean([stock.get(f"{m}_zscore", 0) for m in momentum_metrics])
        stock['technical_score'] = (stock['bollinger_score']) # Average of scores
        stock['volatility_score'] = (stock['bollinger_vol_score'])  # Average of scores
        stock['baseline_score'] = np.mean([stock.get(f"{m}_zscore", 0) for m in baseline_metrics]) #don't remove for some reason it will srew up core score
        stock["core_score"] = np.mean([stock.get(f"{m}_zscore", 0) for m in core_metrics])
    
    # Calculate composite scores
    
      # Define your inputs (X) and target (y)
    X = np.array([[stock["growth_score"], stock["growth_score"], stock["value_score"], stock["momentum_score"], stock["technical_score"], stock["volatility_score"], stock["baseline_score"]] for stock in stock_dicts ])
    y = np.array([stock["core_score"] for stock in stock_dicts])  # Replace with your target variable

    # Handle NaN values in X using SimpleImputer
    imputer = SimpleImputer(strategy="mean")
    X = imputer.fit_transform(X)

    # Split your data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Create and fit your model
    model = RandomForestRegressor()
    model.fit(X_train, y_train)

    # Evaluate your model
    y_pred = model.predict(X_test)
    print(f"Mean Squared Error: {mean_squared_error(y_test, y_pred)}")

    # The feature importances can be taken as the weights
    weights = model.feature_importances_
    print(f"Weights: {weights}")

    conn = pymysql.connect(host='localhost', user='stocks', password='bed0elAn', database='stocks')
    insert_stock_scores(conn, stock_dicts)
    
    # Now use these weights when calculating the composite score:
    for stock in stock_dicts:
        stock["composite_score"] = weights[0]*stock["quality_score"] + weights[1]*stock["value_score"] + weights[2]*stock["growth_score"] + weights[3]*stock["momentum_score"] + weights[4]*stock["technical_score"] + weights[5]*stock["baseline_score"] + weights[6]*stock["core_score"]


    
    sorted_stocks_composite = sorted(stock_dicts, key=lambda x: x["composite_score"], reverse=True)
    sorted_stocks_quality = sorted(stock_dicts, key=lambda x: x["quality_score"], reverse=True)
    sorted_stocks_value = sorted(stock_dicts, key=lambda x: x["value_score"], reverse=True)
    sorted_stocks_growth = sorted(stock_dicts, key=lambda x: x["growth_score"], reverse=True)
    sorted_stocks_momentum = sorted(stock_dicts, key=lambda x: x["momentum_score"], reverse=True)
    sorted_stocks_technical = sorted(stock_dicts, key=lambda x: x["technical_score"], reverse=True)
    sorted_stocks_volatility = sorted(stock_dicts, key=lambda x: x["volatility_score"], reverse=True)
    sorted_stocks_baseline = sorted(stock_dicts, key=lambda x: x["baseline_score"], reverse=True)
    sorted_stocks_core = sorted(stock_dicts, key=lambda x: x["core_score"], reverse=True)
    print(f"\nTop 10 stocks for sector {sector} by each metric:")


#    print("\nComposite Score:")
#    for stock in sorted_stocks_composite[:10]:
#        print(f"{stock['symbol']} (Composite: {stock['composite_score']:.2f})")

#    print("\nQuality Score:")
#    for stock in sorted_stocks_quality[:10]:
#        print(f"{stock['symbol']} (Quality: {stock['quality_score']:.2f})")

#    print("\nValue Score:")
#   for stock in sorted_stocks_value[:10]:
#        print(f"{stock['symbol']} (Value: {stock['value_score']:.2f})")

#    print("\nGrowth Score:")
#    for stock in sorted_stocks_growth[:5]:
#        print(f"{stock['symbol']} (Growth: {stock['growth_score']:.2f})")

#    print("\nMomentum Score:")
#    for stock in sorted_stocks_momentum[:10]:
#        print(f"{stock['symbol']} (Momentum: {stock['momentum_score']:.2f})")

#    print("\nVolatility Score:")
#    for stock in sorted_stocks_technical[:10]:
#        print(f"{stock['symbol']} (Technical: {stock['volatility_score']:.2f})")

#    print("\nTechnical Score:")
#    for stock in sorted_stocks_technical[:10]:
#        print(f"{stock['symbol']} (Technical: {stock['technical_score']:.2f})")
#        print("\n")

#    print("\nbaseline Score:")
#    for stock in sorted_stocks_baseline[:10]:
#        print(f"{stock['symbol']} (baseline: {stock['baseline_score']:.2f})")
#        print("\n")

#    print("\nTechnical Score (Top 5):")
#    for stock in sorted_stocks_technical[:5]:
#        print(f"{stock['symbol']} (Technical: {stock['technical_score']:.2f})")
#        for metric in technical_metrics:
#            print(f"{metric}: {stock[metric]}")
#        print("\n")

#    print("\nTechnical Score (Bottom 5):")
#    for stock in sorted_stocks_technical[-5:]:  # Slicing from the end to get bottom 5 stocks
#        print(f"{stock['symbol']} (Technical: {stock['technical_score']:.2f})")
#        for metric in technical_metrics:
#            print(f"{metric}: {stock[metric]}")
#        print("\n")

    #Print the top 10 stocks
#    print("Top 10 scores:")
#    top_stocks = sorted(stock_dicts, key=lambda x: x['put_call_ratio'], reverse=True)[:10]
#    for stock in top_stocks:
#        print(f"{stock['symbol']}: {stock['put_call_ratio']}")

    #Print the bottom 10 stocks
#    print("\nBottom 10 scores:")
#    bottom_stocks = sorted(stock_dicts, key=lambda x: x['put_call_ratio'])[:10]
#    for stock in bottom_stocks:
#        print(f"{stock['symbol']}: {stock['put_call_ratio']}")


# Close the connection to the MySQL database
cursor.close()
conn.close()

