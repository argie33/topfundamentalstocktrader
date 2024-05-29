import requests
import pandas as pd
import time
import pymysql
import talib
import numpy as np
from datetime import datetime, timedelta


def get_with_retry(url, retries=3, delay=1):
    for _ in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()  # This will raise an HTTPError if the response was unsuccessful
            return response
        except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
            print(f"Request failed with {e}, retrying...")
            time.sleep(delay)
    raise Exception(f"Failed to fetch data from {url} after {retries} retries")


# Database credentials
db_host = 'localhost'
db_user = 'user'
db_password = 'password'
db_name = 'db'

# Connect to the MySQL database
conn = pymysql.connect(host=db_host, user=db_user, password=db_password, db=db_name)
cursor = conn.cursor()

# Data feed setup
api_key = 'yourapikey'

# Get all unique stock symbols along with their sector and industry
cursor.execute("SELECT DISTINCT symbol, sector, index_name FROM stock_symbols")
symbols_with_sector_and_industry = cursor.fetchall()

# Filter symbols that have 'N/A' for both sector and industry
symbols = [(symbol) for symbol in symbols_with_sector_and_industry]

# Drop the existing 'ta' table if it exists
cursor.execute("DROP TABLE IF EXISTS technical_data")
conn.commit()

# Create table 'ta' with new structure
cursor.execute("""
    CREATE TABLE IF NOT EXISTS technical_data (
        symbol VARCHAR(10),
        index_name VARCHAR(255),
        date DATE,
        open_price FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume BIGINT,
        sma_volume_10 FLOAT,
        roc FLOAT,
        rsi FLOAT,
        macd FLOAT,
        macd_signal FLOAT,
        macd_hist FLOAT,
        ad FLOAT,
        adosc FLOAT,
        adx FLOAT,
        adxr FLOAT,
        apo FLOAT,
        aroon_up FLOAT,
        aroon_down FLOAT,
        aroonosc FLOAT,
        atr FLOAT,
        avgprice FLOAT,
        bbands_upper FLOAT,
        bbands_middle FLOAT,
        bbands_lower FLOAT,
        bop FLOAT,
        cci FLOAT,
        min FLOAT,
        max FLOAT,
        cmo FLOAT,
        correl FLOAT,
        dema FLOAT,              
        dx FLOAT,
        ema FLOAT,
        ema_10 FLOAT,
        ema_20 FLOAT,
        ema_50 FLOAT,
        ema_100 FLOAT,
        ema_150 FLOAT,
        ema_200 FLOAT,                 
        kama FLOAT,
        ma FLOAT,
        mom FLOAT,
        willr FLOAT,
        sar FLOAT,
        ultosc FLOAT,   
        tsf FLOAT,
        natr FLOAT,
        obv FLOAT,
        tenkan_sen FLOAT,
        kijun_sen FLOAT,
        senkou_span_a FLOAT,
        senkou_span_b FLOAT,
        chikou_span FLOAT,
        stdev FLOAT,
        sma_10 FLOAT,
        sma_20 FLOAT,
        sma_50 FLOAT,
        sma_100 FLOAT,
        sma_150 FLOAT,
        sma_200 FLOAT,
        roc_sma_50 FLOAT,
        roc_sma_200 FLOAT,
        mfi FLOAT,
        vwap FLOAT,
        bbands_percent_b FLOAT,
        prev_close FLOAT,
        macd_prev FLOAT,
        macd_hist_prev FLOAT,
        macd_signal_prev FLOAT,
        pct_diff_50d_sma FLOAT,
        pct_diff_200d_sma FLOAT,
        relative_close_spy FLOAT,
        relative_price_rsi FLOAT,
        relative_price_macd FLOAT,
        relative_price_macd_signal FLOAT,
        relative_price_macd_hist FLOAT,
        relative_price_roc FLOAT,
        relative_price_ad FLOAT,
        relative_price_adosc FLOAT,
        relative_price_adx FLOAT,
        relative_price_adxr FLOAT,
        relative_price_apo FLOAT,
        relative_price_aroon_up FLOAT,
        relative_price_aroon_down FLOAT,
        relative_price_aroonosc FLOAT,
        relative_price_atr FLOAT,
        relative_price_avgprice FLOAT,
        relative_price_bbands_upper FLOAT,
        relative_price_bbands_middle FLOAT,
        relative_price_bbands_lower FLOAT,
        relative_price_bop FLOAT,
        relative_price_cci FLOAT,
        relative_price_min FLOAT,
        relative_price_max FLOAT,
        relative_price_cmo FLOAT,
        relative_price_correl FLOAT,
        relative_price_dema FLOAT,
        relative_price_dx FLOAT,
        relative_price_sma_10 FLOAT,
        relative_price_sma_20 FLOAT,
        relative_price_sma_50 FLOAT,
        relative_price_sma_100 FLOAT,
        relative_price_sma_150 FLOAT,
        relative_price_sma_200 FLOAT,
        relative_price_ema FLOAT,
        relative_price_ema_10 FLOAT,
        relative_price_ema_20 FLOAT,                        
        relative_price_ema_50 FLOAT,  
        relative_price_ema_100 FLOAT,  
        relative_price_ema_150 FLOAT,  
        relative_price_ema_200 FLOAT,
        relative_price_kama FLOAT,
        relative_price_ma FLOAT,
        relative_price_mom FLOAT,
        relative_price_willr FLOAT,
        relative_price_sar FLOAT,
        relative_price_ultosc FLOAT,
        relative_price_tsf FLOAT,
        relative_price_natr FLOAT,
        relative_price_obv FLOAT, 
        relative_price_mfi FLOAT,
        relative_price_tenkan_sen FLOAT,
        relative_price_kijun_sen FLOAT,
        relative_price_senkou_span_a FLOAT,
        relative_price_senkou_span_b FLOAT,
        relative_price_chikou_span FLOAT,
        relative_price_stdev FLOAT,
        relative_sector_close FLOAT,
        relative_sector_open FLOAT,
        relative_sector_high FLOAT,
        relative_sector_low FLOAT,
        relative_sector_volume FLOAT,
        relative_sector_rsi FLOAT,
        relative_sector_macd FLOAT,
        relative_sector_macd_signal FLOAT,
        relative_sector_macd_hist FLOAT,
        relative_sector_roc FLOAT,
        relative_sector_ad FLOAT,
        relative_sector_adosc FLOAT,
        relative_sector_adx FLOAT,
        relative_sector_adxr FLOAT,
        relative_sector_apo FLOAT,
        relative_sector_aroon_up FLOAT,
        relative_sector_aroon_down FLOAT,
        relative_sector_aroonosc FLOAT,
        relative_sector_atr FLOAT,
        relative_sector_avgprice FLOAT,
        relative_sector_bbands_upper FLOAT,
        relative_sector_bbands_middle FLOAT,
        relative_sector_bbands_lower FLOAT,
        relative_sector_bop FLOAT,
        relative_sector_cci FLOAT,
        relative_sector_min FLOAT,
        relative_sector_max FLOAT,
        relative_sector_cmo FLOAT,
        relative_sector_correl FLOAT,
        relative_sector_dema FLOAT,
        relative_sector_dx FLOAT,
        relative_sector_sma_10 FLOAT,
        relative_sector_sma_20 FLOAT,
        relative_sector_sma_50 FLOAT,
        relative_sector_sma_100 FLOAT,
        relative_sector_sma_150 FLOAT,
        relative_sector_sma_200 FLOAT,
        relative_sector_ema FLOAT,
        relative_sector_ema_10 FLOAT,
        relative_sector_ema_20 FLOAT,
        relative_sector_ema_50 FLOAT,
        relative_sector_ema_100 FLOAT,
        relative_sector_ema_150 FLOAT,
        relative_sector_ema_200 FLOAT,
        relative_sector_kama FLOAT,
        relative_sector_ma FLOAT,
        relative_sector_mom FLOAT,
        relative_sector_willr FLOAT,
        relative_sector_sar FLOAT,
        relative_sector_ultosc FLOAT,
        relative_sector_tsf FLOAT,
        relative_sector_natr FLOAT,
        relative_sector_obv FLOAT,
        relative_sector_mfi FLOAT,
        relative_sector_tenkan_sen FLOAT,
        relative_sector_kijun_sen FLOAT,
        relative_sector_senkou_span_a FLOAT,
        relative_sector_senkou_span_b FLOAT,
        relative_sector_chikou_span FLOAT,
        relative_sector_stdev FLOAT,
        last_updated TIMESTAMP,
        PRIMARY KEY(symbol, date)
    )
""")
conn.commit()

sectorlist = {
    'Financials': 'XLF',
    'Consumer Discretionary': 'XLY',
    'Communication Services': 'XLC',
    'Energy': 'XLE',
    'Industrials': 'XLI',
    'Materials': 'XLB',
    'Real Estate': 'XLRE',
    'Health Care': 'XLV',
    'Utilities': 'XLU',
    'Information Technology': 'XLK',
    'Consumer Staples': 'XLP',
}

end_date = datetime.now()
start_date = end_date - timedelta(days=3*365)  # Fetch data for the last three years
url_spy = f"https://financialmodelingprep.com/api/v3/historical-chart/1day/SPY?from={start_date.strftime('%Y-%m-%d')}&to={end_date.strftime('%Y-%m-%d')}&apikey={api_key}"

# Perform the API request
response_spy = requests.get(url_spy)
if response_spy.status_code == 200:
    data_spy = response_spy.json()
    # Create a DataFrame
    daily_prices_spy = pd.DataFrame(data_spy)
    daily_prices_spy['date'] = pd.to_datetime(daily_prices_spy['date'])
    daily_prices_spy.set_index('date', inplace=True)
else:
    print("Failed to fetch data")
    response_spy.raise_for_status()

# Convert prices to numeric values and sort by index (date)
daily_prices_spy = daily_prices_spy.sort_index()
for col in ['open', 'high', 'low', 'close', 'volume']:
    daily_prices_spy[col] = pd.to_numeric(daily_prices_spy[col])

# Extract the required price data
spy_close = daily_prices_spy['close']
spy_open = daily_prices_spy['open']
spy_high = daily_prices_spy['high']
spy_low = daily_prices_spy['low']
spy_volume = daily_prices_spy['volume']

time.sleep(1)  # Sleep to avoid rate limiting

# Fetch ETF data
etf_data = {}
api_key = 'd288e55d6eef1c72f9a4373d4572c58c'
end_date = datetime.now()
start_date = end_date - timedelta(days=3*365)  # Fetch data for the last three years

for sector1, etf in sectorlist.items():
    print(f"Fetching data for {etf} (ETF for {sector1})...")
    url_etf = f"https://financialmodelingprep.com/api/v3/historical-chart/1day/{etf}?from={start_date.strftime('%Y-%m-%d')}&to={end_date.strftime('%Y-%m-%d')}&apikey={api_key}"

    # Perform the API request
    response_etf = requests.get(url_etf)
    if response_etf.status_code == 200:
        data_etf = response_etf.json()
        # Create a DataFrame
        daily_prices_etf = pd.DataFrame(data_etf)
        daily_prices_etf['date'] = pd.to_datetime(daily_prices_etf['date'])
        daily_prices_etf.set_index('date', inplace=True)
    else:
        print("Failed to fetch data")
        response_etf.raise_for_status()

    # Convert prices to numeric values and sort by index (date)
    daily_prices_etf = daily_prices_etf.sort_index()
    for col in ['open', 'high', 'low', 'close', 'volume']:
        daily_prices_etf[col] = pd.to_numeric(daily_prices_etf[col])

    # Extract the required price data and store in the dictionary
    etf_data[sector1] = {
        'close': daily_prices_etf['close'],
        'open': daily_prices_etf['open'],
        'high': daily_prices_etf['high'],
        'low': daily_prices_etf['low'],
        'volume': daily_prices_etf['volume']
    }


    time.sleep(1)  # Sleep to avoid rate limiting


# Fetch daily price data for each stock
end_date = datetime.now()
start_date = end_date - timedelta(days=3*365)  # Fetch data for the last three years

for symbol, sector, index_name in symbols_with_sector_and_industry:
    print(f"Fetching data for {symbol}...")
    url = f"https://financialmodelingprep.com/api/v3/historical-chart/1day/{symbol}?from={start_date.strftime('%Y-%m-%d')}&to={end_date.strftime('%Y-%m-%d')}&apikey={api_key}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data:  # Check if data list is not empty
                daily_prices = pd.DataFrame(data)
                daily_prices['date'] = pd.to_datetime(daily_prices['date'])
                daily_prices.set_index('date', inplace=True)

                # Convert prices to numeric values and sort by index (date)
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    daily_prices[col] = pd.to_numeric(daily_prices[col], errors='coerce')

                print(f"Data summary for {symbol}:", daily_prices.describe())

                # Ensure no NaNs are present in the data
                if daily_prices.isna().any().any():
                    print(f"Missing data for {symbol}, skipping calculations.")
                    continue

                # Calculate technical indicators
                close = daily_prices['close'].dropna()
                open_price = daily_prices['open'].dropna()
                high = daily_prices['high'].dropna()
                low = daily_prices['low'].dropna()
                volume = daily_prices['volume'].dropna()

                if close.empty or high.empty or low.empty or volume.empty:
                    print(f"Not enough valid data for {symbol} to calculate indicators.")
                    continue

            roc = talib.ROC(close, timeperiod = 10)
            rsi = talib.RSI(close, timeperiod=14)
            macd, macd_signal, macd_hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
            ad = talib.AD(high, low, close, volume)
            adosc = talib.ADOSC(high, low, close, volume, fastperiod=3, slowperiod=10)
            adx = talib.ADX(high, low, close, timeperiod=14)
            adxr = talib.ADXR(high, low, close, timeperiod=14)
            apo = talib.APO(close, fastperiod=12, slowperiod=26, matype=0)
            aroon_up, aroon_down = talib.AROON(high, low, timeperiod=14)
            aroonosc = talib.AROONOSC(high, low, timeperiod=14)
            atr = talib.ATR(high, low, close, timeperiod=14)
            avgprice = talib.AVGPRICE(open_price, high, low, close)
            bbands_upper, bbands_middle, bbands_lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
            bop = talib.BOP(open_price, high, low, close)
            cci = talib.CCI(high, low, close, timeperiod=14)
            min_max = talib.MINMAX(close, timeperiod=30)
            cmo = talib.CMO(close, timeperiod=14)
            correl = talib.CORREL(high, low, timeperiod=30)
            dema = talib.DEMA(close, timeperiod=30)
            dx = talib.DX(high, low, close, timeperiod=14)
            ema = talib.EMA(close, timeperiod=13)
            ema_10 = talib.EMA(close, timeperiod=10)
            ema_20 = talib.EMA(close, timeperiod=20)
            ema_50 = talib.EMA(close, timeperiod=50)
            ema_100 = talib.EMA(close, timeperiod=100)
            ema_150 = talib.EMA(close, timeperiod=150)
            ema_200 = talib.EMA(close, timeperiod=200)
            kama = talib.KAMA(close, timeperiod=30)
            ma = talib.MA(close, timeperiod=30, matype=0)
            mom = talib.MOM(close, timeperiod=10)
            willr = talib.WILLR(high, low, close, timeperiod=14)
            sar = talib.SAR(high, low, acceleration=0.02, maximum=0.2)
            ultosc = talib.ULTOSC(high, low, close, timeperiod1=7, timeperiod2=14, timeperiod3=28)
            tsf = talib.TSF(close, timeperiod=14)
            natr = talib.NATR(high, low, close, timeperiod=14)
            obv = talib.OBV(close, volume)
            tenkan_sen = (talib.MAX(high, 9) + talib.MIN(low, 9)) / 2
            kijun_sen = (talib.MAX(high, 26) + talib.MIN(low, 26)) / 2
            senkou_span_a = (tenkan_sen + kijun_sen) / 2
            senkou_span_b = (talib.MAX(high, 52) + talib.MIN(low, 52)) / 2
            chikou_span = close.shift(-26)
            stdev = talib.STDDEV(close, timeperiod=5)
            sma_10 = talib.SMA(close, timeperiod=10)
            sma_20 = talib.SMA(close, timeperiod=20)
            sma_50 = talib.SMA(close, timeperiod=50)
            sma_100 = talib.SMA(close, timeperiod=100)
            sma_150 = talib.SMA(close, timeperiod=150)                
            sma_200 = talib.SMA(close, timeperiod=200)
            roc_sma_50 = talib.ROC(sma_50, timeperiod=1)
            roc_sma_200 = talib.ROC(sma_200, timeperiod=1)
            mfi = talib.MFI(high, low, close, volume, timeperiod=14)
            typical_price = (high + low + close) / 3
            vwap = np.cumsum(volume * typical_price) / np.cumsum(volume)
            prev_close = close.shift(1)
            sma_volume_10 = talib.SMA(volume, timeperiod=10)
            bbands_upper, bbands_middle, bbands_lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
            bbands_percent_b = (close - bbands_lower) / (bbands_upper - bbands_lower)
            macd_prev = macd.shift(1)
            macd_hist_prev = macd_hist.shift(1)
            macd_signal_prev = macd_signal.shift(1)

            # Calculate the percentage difference between the close price and the 50-day and 200-day SMA
            pct_diff_50d_sma = (close - sma_50) / sma_50 * 100
            pct_diff_200d_sma = (close - sma_200) / sma_200 * 100

            relative_close_spy = close / spy_close.reindex(close.index)
            relative_open_spy = open_price / spy_open.reindex(open_price.index)
            relative_high_spy = high / spy_high.reindex(high.index)
            relative_low_spy = low / spy_low.reindex(low.index)
            relative_volume_spy = volume / spy_volume.reindex(volume.index)

            relative_price_rsi = talib.RSI(relative_close_spy, timeperiod=14)
            relative_price_macd, relative_price_macd_signal, relative_price_macd_hist = talib.MACD(relative_close_spy, fastperiod=12, slowperiod=26, signalperiod=9)
            relative_price_roc = talib.ROC(relative_close_spy, timeperiod=10)
            relative_price_ad = talib.AD(relative_high_spy, relative_low_spy, relative_close_spy, relative_volume_spy)
            relative_price_adosc = talib.ADOSC(relative_high_spy, relative_low_spy, relative_close_spy, relative_volume_spy, fastperiod=3, slowperiod=10)
            relative_price_adx = talib.ADX(relative_high_spy, relative_low_spy, relative_close_spy, timeperiod=14)
            relative_price_adxr = talib.ADXR(relative_high_spy, relative_low_spy, relative_close_spy, timeperiod=14)
            relative_price_apo = talib.APO(relative_close_spy, fastperiod=12, slowperiod=26, matype=0)
            relative_price_aroon_up, relative_price_aroon_down = talib.AROON(relative_high_spy, relative_low_spy, timeperiod=14)
            relative_price_aroonosc = talib.AROONOSC(relative_high_spy, relative_low_spy, timeperiod=14)
            relative_price_atr = talib.ATR(relative_high_spy, relative_low_spy, relative_close_spy, timeperiod=14)
            relative_price_avgprice = talib.AVGPRICE(relative_open_spy, relative_high_spy, relative_low_spy, relative_close_spy)
            relative_price_bbands_upper, relative_price_bbands_middle, relative_price_bbands_lower = talib.BBANDS(relative_close_spy, timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)
            relative_price_bop = talib.BOP(relative_open_spy, relative_high_spy, relative_low_spy, relative_close_spy)
            relative_price_cci = talib.CCI(relative_high_spy, relative_low_spy, relative_close_spy, timeperiod=14)
            relative_price_min = relative_close_spy.rolling(window=14).min()
            relative_price_max = relative_close_spy.rolling(window=14).max()
            relative_price_cmo = talib.CMO(relative_close_spy, timeperiod=14)
            relative_price_correl = talib.CORREL(relative_high_spy, relative_close_spy, timeperiod=30)
            relative_price_dema = talib.DEMA(relative_close_spy, timeperiod=30)
            relative_price_dx = talib.DX(relative_high_spy, relative_low_spy, relative_close_spy, timeperiod=14)
            relative_price_sma_10 = talib.SMA(relative_close_spy, timeperiod=10)
            relative_price_sma_20 = talib.SMA(relative_close_spy, timeperiod=20)
            relative_price_sma_50 = talib.SMA(relative_close_spy, timeperiod=50)
            relative_price_sma_100 = talib.SMA(relative_close_spy, timeperiod=100)
            relative_price_sma_150 = talib.SMA(relative_close_spy, timeperiod=150)
            relative_price_sma_200 = talib.SMA(relative_close_spy, timeperiod=200)
            relative_price_ema = talib.EMA(relative_close_spy, timeperiod=30)
            relative_price_ema_10 = talib.EMA(relative_close_spy, timeperiod=10)
            relative_price_ema_20 = talib.EMA(relative_close_spy, timeperiod=20)
            relative_price_ema_50 = talib.EMA(relative_close_spy, timeperiod=50)
            relative_price_ema_100 = talib.EMA(relative_close_spy, timeperiod=100)
            relative_price_ema_150 = talib.EMA(relative_close_spy, timeperiod=150)
            relative_price_ema_200 = talib.EMA(relative_close_spy, timeperiod=200)
            relative_price_kama = talib.KAMA(relative_close_spy, timeperiod=30)
            relative_price_ma = talib.MA(relative_close_spy, timeperiod=30)
            relative_price_mom = talib.MOM(relative_close_spy, timeperiod=10)
            relative_price_willr = talib.WILLR(relative_high_spy, relative_low_spy, relative_close_spy, timeperiod=14)
            relative_price_sar = talib.SAR(relative_high_spy, relative_low_spy, acceleration=0.02, maximum=0.2)
            relative_price_ultosc = talib.ULTOSC(relative_high_spy, relative_low_spy, relative_close_spy, timeperiod1=7, timeperiod2=14, timeperiod3=28)
            relative_price_tsf = talib.TSF(relative_close_spy, timeperiod=14)
            relative_price_natr = talib.NATR(relative_high_spy, relative_low_spy, relative_close_spy, timeperiod=14)
            relative_price_obv = talib.OBV(relative_close_spy, relative_volume_spy)
            relative_price_mfi = talib.MFI(relative_high_spy, relative_low_spy, relative_close_spy, relative_volume_spy, timeperiod=14)
            relative_price_tenkan_sen = (talib.MAX(relative_close_spy, 9) + talib.MIN(relative_close_spy, 9)) / 2
            relative_price_kijun_sen = (talib.MAX(relative_close_spy, 26) + talib.MIN(relative_close_spy, 26)) / 2
            relative_price_senkou_span_a = (relative_price_tenkan_sen + relative_price_kijun_sen) / 2
            relative_price_senkou_span_b = (talib.MAX(relative_close_spy, 52) + talib.MIN(relative_close_spy, 52)) / 2
            relative_price_chikou_span = relative_close_spy.shift(-26)
            relative_price_stdev = talib.STDDEV(relative_close_spy, timeperiod=10)

            etf_close = etf_data[sector1]['close']
            relative_sector_close = close / etf_close.reindex(close.index)
            etf_open = etf_data[sector1]['open']
            relative_sector_open = open_price / etf_open.reindex(open_price.index)
            etf_high = etf_data[sector1]['high']
            relative_sector_high = high / etf_high.reindex(high.index)
            etf_low = etf_data[sector1]['low']
            relative_sector_low = low / etf_low.reindex(low.index)
            etf_volume = etf_data[sector1]['volume']
            relative_sector_volume = volume / etf_volume.reindex(volume.index)

            relative_sector_rsi = talib.RSI(relative_sector_close, timeperiod=14)
            relative_sector_macd, relative_sector_macd_signal, relative_sector_macd_hist = talib.MACD(relative_sector_close, fastperiod=12, slowperiod=26, signalperiod=9)
            relative_sector_roc = talib.ROC(relative_sector_close, timeperiod=10)
            relative_sector_ad = talib.AD(relative_sector_high, relative_sector_low, relative_sector_close, relative_sector_volume)
            relative_sector_adosc = talib.ADOSC(relative_sector_high, relative_sector_low, relative_sector_close, relative_sector_volume, fastperiod=3, slowperiod=10)
            relative_sector_adx = talib.ADX(relative_sector_high, relative_sector_low, relative_sector_close, timeperiod=14)
            relative_sector_adxr = talib.ADXR(relative_sector_high, relative_sector_low, relative_sector_close, timeperiod=14)
            relative_sector_apo = talib.APO(relative_sector_close, fastperiod=12, slowperiod=26, matype=0)
            relative_sector_aroon_up, relative_sector_aroon_down = talib.AROON(relative_sector_high, relative_sector_low, timeperiod=14)
            relative_sector_aroonosc = talib.AROONOSC(relative_sector_high, relative_sector_low, timeperiod=14)
            relative_sector_atr = talib.ATR(relative_sector_high, relative_sector_low, relative_sector_close, timeperiod=14)
            relative_sector_avgprice = talib.AVGPRICE(relative_sector_open, relative_sector_high, relative_sector_low, relative_sector_close)
            relative_sector_bbands_upper, relative_sector_bbands_middle, relative_sector_bbands_lower = talib.BBANDS(relative_sector_close, timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)
            relative_sector_bop = talib.BOP(relative_sector_open, relative_sector_high, relative_sector_low, relative_sector_close)
            relative_sector_cci = talib.CCI(relative_sector_high, relative_sector_low, relative_sector_close, timeperiod=14)
            relative_sector_min = relative_sector_close.rolling(window=14).min()
            relative_sector_max = relative_sector_close.rolling(window=14).max()
            relative_sector_cmo = talib.CMO(relative_sector_close, timeperiod=14)
            relative_sector_correl = talib.CORREL(relative_sector_high, relative_sector_close, timeperiod=30)
            relative_sector_dema = talib.DEMA(relative_sector_close, timeperiod=30)
            relative_sector_dx = talib.DX(relative_sector_high, relative_sector_low, relative_sector_close, timeperiod=14)
            relative_sector_sma_10 = talib.SMA(relative_sector_close, timeperiod=10)
            relative_sector_sma_20 = talib.SMA(relative_sector_close, timeperiod=20)
            relative_sector_sma_50 = talib.SMA(relative_sector_close, timeperiod=50)
            relative_sector_sma_100 = talib.SMA(relative_sector_close, timeperiod=100)
            relative_sector_sma_150 = talib.SMA(relative_sector_close, timeperiod=150)
            relative_sector_sma_200 = talib.SMA(relative_sector_close, timeperiod=200)
            relative_sector_ema = talib.EMA(relative_sector_close, timeperiod=30)
            relative_sector_ema_10 = talib.EMA(relative_sector_close, timeperiod=10)
            relative_sector_ema_20 = talib.EMA(relative_sector_close, timeperiod=20)
            relative_sector_ema_50 = talib.EMA(relative_sector_close, timeperiod=50)
            relative_sector_ema_100 = talib.EMA(relative_sector_close, timeperiod=100)
            relative_sector_ema_150 = talib.EMA(relative_sector_close, timeperiod=150)
            relative_sector_ema_200 = talib.EMA(relative_sector_close, timeperiod=200)
            relative_sector_kama = talib.KAMA(relative_sector_close, timeperiod=30)
            relative_sector_ma = talib.MA(relative_sector_close, timeperiod=30)
            relative_sector_mom = talib.MOM(relative_sector_close, timeperiod=10)
            relative_sector_willr = talib.WILLR(relative_sector_high, relative_sector_low, relative_sector_close, timeperiod=14)
            relative_sector_sar = talib.SAR(relative_sector_high, relative_sector_low, acceleration=0.02, maximum=0.2)
            relative_sector_ultosc = talib.ULTOSC(relative_sector_high, relative_sector_low, relative_sector_close, timeperiod1=7, timeperiod2=14, timeperiod3=28)
            relative_sector_tsf = talib.TSF(relative_sector_close, timeperiod=14)
            relative_sector_natr = talib.NATR(relative_sector_high, relative_sector_low, relative_sector_close, timeperiod=14)
            relative_sector_obv = talib.OBV(relative_sector_close, relative_sector_volume)
            relative_sector_mfi = talib.MFI(relative_sector_high, relative_sector_low, relative_sector_close, relative_sector_volume, timeperiod=14)
            relative_sector_tenkan_sen = (talib.MAX(relative_sector_close, 9) + talib.MIN(relative_sector_close, 9)) / 2
            relative_sector_kijun_sen = (talib.MAX(relative_sector_close, 26) + talib.MIN(relative_sector_close, 26)) / 2
            relative_sector_senkou_span_a = (relative_sector_tenkan_sen + relative_sector_kijun_sen) / 2
            relative_sector_senkou_span_b = (talib.MAX(relative_sector_close, 52) + talib.MIN(relative_sector_close, 52)) / 2
            relative_sector_chikou_span = relative_sector_close.shift(-26)
            relative_sector_stdev = talib.STDDEV(relative_sector_close, timeperiod=10)


            # Combine all series into a DataFrame
            indicators = pd.DataFrame({
                'symbol': symbol,
                'index_name': index_name,
                'date': close.index,
                'open_price': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume,
                'sma_volume_10': sma_volume_10,
                'roc': roc,
                'rsi': rsi,
                'macd': macd,
                'macd_signal':macd_signal,
                'macd_hist':macd_hist,
                'ad': ad,
                'adosc': adosc,
                'adx': adx,
                'adxr': adxr,
                'apo': apo,
                'aroon_up': aroon_up,
                'aroon_down': aroon_down,
                'aroonosc': aroonosc,
                'atr': atr,
                'avgprice': avgprice,
                'bbands_upper': bbands_upper,
                'bbands_middle': bbands_middle,
                'bbands_lower': bbands_lower,
                'bop': bop,
                'cci': cci,
                'min': min_max[0],
                'max': min_max[1],
                'cmo': cmo,
                'correl': correl,
                'dema': dema,
                'dx': dx,
                'ema': ema,
                'ema_10': ema_10,
                'ema_20': ema_20,                        
                'ema_50': ema_50,  
                'ema_100': ema_100,  
                'ema_150': ema_150,  
                'ema_200': ema_200,  
                'kama': kama,
                'ma': ma,
                'mom': mom,
                'willr': willr,
                'sar': sar,
                'ultosc': ultosc,
                'tsf': tsf,
                'natr': natr,
                'obv': obv,
                'tenkan_sen': tenkan_sen,
                'kijun_sen': kijun_sen,
                'senkou_span_a': senkou_span_a,
                'senkou_span_b': senkou_span_b,
                'chikou_span': chikou_span,
                'stdev': stdev,
                'sma_10': sma_10,
                'sma_20': sma_20,
                'sma_50': sma_50,
                'sma_100': sma_100,
                'sma_150': sma_150,
                'sma_200': sma_200,
                'roc_sma_50': roc_sma_50,
                'roc_sma_200': roc_sma_200,
                'mfi': mfi,
                'vwap':vwap,
                'bbands_percent_b': bbands_percent_b,
                'macd_prev': macd_prev,
                'macd_hist_prev': macd_hist_prev,
                'macd_signal_prev': macd_signal_prev,
                'prev_close': prev_close,
                'pct_diff_50d_sma': pct_diff_50d_sma,
                'pct_diff_200d_sma': pct_diff_200d_sma,
                'relative_close_spy' : relative_close_spy,
                'relative_price_rsi': relative_price_rsi,
                'relative_price_macd': relative_price_macd,
                'relative_price_macd_signal': relative_price_macd_signal,
                'relative_price_macd_hist': relative_price_macd_hist,
                'relative_price_roc': relative_price_roc,
                'relative_price_ad': relative_price_ad,
                'relative_price_adosc': relative_price_adosc,
                'relative_price_adx': relative_price_adx,
                'relative_price_adxr': relative_price_adxr,
                'relative_price_apo': relative_price_apo,
                'relative_price_aroon_up': relative_price_aroon_up,
                'relative_price_aroon_down': relative_price_aroon_down,
                'relative_price_aroonosc': relative_price_aroonosc,
                'relative_price_atr': relative_price_atr,
                'relative_price_avgprice': relative_price_avgprice,
                'relative_price_bbands_upper': relative_price_bbands_upper,
                'relative_price_bbands_middle': relative_price_bbands_middle,
                'relative_price_bbands_lower': relative_price_bbands_lower,
                'relative_price_bop': relative_price_bop,
                'relative_price_cci': relative_price_cci,
                'relative_price_min': relative_price_min,
                'relative_price_max': relative_price_max,
                'relative_price_cmo': relative_price_cmo,
                'relative_price_correl': relative_price_correl,
                'relative_price_dema': relative_price_dema,
                'relative_price_dx': relative_price_dx,
                'relative_price_ema': relative_price_ema,
                'relative_price_sma_10': relative_price_sma_10,
                'relative_price_sma_20': relative_price_sma_20,
                'relative_price_sma_50': relative_price_sma_50,
                'relative_price_sma_100': relative_price_sma_100,
                'relative_price_sma_150': relative_price_sma_150,
                'relative_price_sma_200': relative_price_sma_200,
                'relative_price_ema_10': relative_price_ema_10,
                'relative_price_ema_20': relative_price_ema_20,                        
                'relative_price_ema_50': relative_price_ema_50,  
                'relative_price_ema_100': relative_price_ema_100,  
                'relative_price_ema_150': relative_price_ema_150,  
                'relative_price_ema_200': relative_price_ema_200,  
                'relative_price_kama': relative_price_kama,
                'relative_price_ma': relative_price_ma,
                'relative_price_mom': relative_price_mom,
                'relative_price_willr': relative_price_willr,
                'relative_price_sar': relative_price_sar,
                'relative_price_ultosc': relative_price_ultosc,
                'relative_price_tsf': relative_price_tsf,
                'relative_price_natr': relative_price_natr,
                'relative_price_obv': relative_price_obv,
                'relative_price_mfi': relative_price_mfi,
                'relative_price_tenkan_sen': relative_price_tenkan_sen,
                'relative_price_kijun_sen': relative_price_kijun_sen,
                'relative_price_senkou_span_a': relative_price_senkou_span_a,
                'relative_price_senkou_span_b': relative_price_senkou_span_b,
                'relative_price_chikou_span': relative_price_chikou_span,
                'relative_price_stdev': relative_price_stdev,
                'relative_sector_close': relative_sector_close,
                'relative_sector_open': relative_sector_open,
                'relative_sector_high': relative_sector_high,
                'relative_sector_low': relative_sector_low,
                'relative_sector_volume': relative_sector_volume,
                'relative_sector_rsi': relative_sector_rsi,
                'relative_sector_macd': relative_sector_macd,
                'relative_sector_macd_signal': relative_sector_macd_signal,
                'relative_sector_macd_hist': relative_sector_macd_hist,
                'relative_sector_roc': relative_sector_roc,
                'relative_sector_ad': relative_sector_ad,
                'relative_sector_adosc': relative_sector_adosc,
                'relative_sector_adx': relative_sector_adx,
                'relative_sector_adxr': relative_sector_adxr,
                'relative_sector_apo': relative_sector_apo,
                'relative_sector_aroon_up': relative_sector_aroon_up,
                'relative_sector_aroon_down': relative_sector_aroon_down,
                'relative_sector_aroonosc': relative_sector_aroonosc,
                'relative_sector_atr': relative_sector_atr,
                'relative_sector_avgprice': relative_sector_avgprice,
                'relative_sector_bbands_upper': relative_sector_bbands_upper,
                'relative_sector_bbands_middle': relative_sector_bbands_middle,
                'relative_sector_bbands_lower': relative_sector_bbands_lower,
                'relative_sector_bop': relative_sector_bop,
                'relative_sector_cci': relative_sector_cci,
                'relative_sector_min': relative_sector_min,
                'relative_sector_max': relative_sector_max,
                'relative_sector_cmo': relative_sector_cmo,
                'relative_sector_correl': relative_sector_correl,
                'relative_sector_dema': relative_sector_dema,
                'relative_sector_dx': relative_sector_dx,
                'relative_sector_sma_10': relative_sector_sma_10,
                'relative_sector_sma_20': relative_sector_sma_20,
                'relative_sector_sma_50': relative_sector_sma_50,
                'relative_sector_sma_100': relative_sector_sma_100,
                'relative_sector_sma_150': relative_sector_sma_150,
                'relative_sector_sma_200': relative_sector_sma_200,
                'relative_sector_ema': relative_sector_ema,
                'relative_sector_ema_10': relative_sector_ema_10,
                'relative_sector_ema_20': relative_sector_ema_20,
                'relative_sector_ema_50': relative_sector_ema_50,
                'relative_sector_ema_100': relative_sector_ema_100,
                'relative_sector_ema_150': relative_sector_ema_150,
                'relative_sector_ema_200': relative_sector_ema_200,
                'relative_sector_kama': relative_sector_kama,
                'relative_sector_ma': relative_sector_ma,
                'relative_sector_mom': relative_sector_mom,
                'relative_sector_willr': relative_sector_willr,
                'relative_sector_sar': relative_sector_sar,
                'relative_sector_ultosc': relative_sector_ultosc,
                'relative_sector_tsf': relative_sector_tsf,
                'relative_sector_natr': relative_sector_natr,
                'relative_sector_obv': relative_sector_obv,
                'relative_sector_mfi': relative_sector_mfi,
                'relative_sector_tenkan_sen': relative_sector_tenkan_sen,
                'relative_sector_kijun_sen': relative_sector_kijun_sen,
                'relative_sector_senkou_span_a': relative_sector_senkou_span_a,
                'relative_sector_senkou_span_b': relative_sector_senkou_span_b,
                'relative_sector_chikou_span': relative_sector_chikou_span,
                'relative_sector_stdev': relative_sector_stdev
            })

            # Drop rows with any missing values
            indicators = indicators.replace({np.nan: None})

            # Insert each row into the 'ta' table in the database
            for i, row in indicators.iterrows():
                row = row.to_dict()  # Convert the row to a dictionary
                cursor.execute("""
                    INSERT INTO technical_data (
                        symbol, index_name, date, open_price, high, low, close, volume, sma_volume_10, roc, rsi, macd, macd_signal, macd_hist, ad, adosc, adx, adxr, apo, aroon_up,
                        aroon_down, aroonosc, atr, avgprice, bbands_upper, bbands_middle, bbands_lower, bop, cci, min, max, cmo, correl,
                        dema, dx, ema, ema_10, ema_20, ema_50, ema_100, ema_150, ema_200, kama, ma, mom, willr, sar, ultosc, tsf, natr, obv, tenkan_sen, kijun_sen, senkou_span_a, senkou_span_b, chikou_span, stdev, 
                        sma_10, sma_20, sma_50, sma_100, sma_150, sma_200, roc_sma_50, roc_sma_200, mfi, vwap, bbands_percent_b, prev_close, macd_prev, macd_hist_prev, macd_signal_prev, pct_diff_50d_sma, pct_diff_200d_sma, 
                        relative_close_spy, relative_price_rsi, relative_price_macd, relative_price_macd_signal, 
                        relative_price_macd_hist, relative_price_roc, relative_price_ad, relative_price_adosc, relative_price_adx, relative_price_adxr, relative_price_apo, relative_price_aroon_up, relative_price_aroon_down, relative_price_aroonosc, 
                        relative_price_atr, relative_price_avgprice, relative_price_bbands_upper, relative_price_bbands_middle, relative_price_bbands_lower, relative_price_bop, relative_price_cci, 
                        relative_price_min, relative_price_max, relative_price_cmo, relative_price_correl, relative_price_dema, relative_price_dx, relative_price_ema,     relative_price_sma_10,
                        relative_price_sma_20, relative_price_sma_50, relative_price_sma_100, relative_price_sma_150, relative_price_sma_200, 
                        relative_price_ema_10, relative_price_ema_20, relative_price_ema_50, relative_price_ema_100, relative_price_ema_150, relative_price_ema_200,  
                        relative_price_kama, relative_price_ma, relative_price_mom, relative_price_willr, relative_price_sar, relative_price_ultosc, relative_price_tsf, relative_price_natr, relative_price_obv, 
                        relative_price_mfi, relative_price_tenkan_sen, relative_price_kijun_sen, relative_price_senkou_span_a, relative_price_senkou_span_b, relative_price_chikou_span, relative_price_stdev, 
                        relative_sector_close, relative_sector_open, relative_sector_high, relative_sector_low, relative_sector_volume, 
                        relative_sector_rsi, relative_sector_macd, relative_sector_macd_signal, relative_sector_macd_hist, relative_sector_roc, relative_sector_ad,
                        relative_sector_adosc, relative_sector_adx, relative_sector_adxr, relative_sector_apo, relative_sector_aroon_up, relative_sector_aroon_down, relative_sector_aroonosc, relative_sector_atr,
                        relative_sector_avgprice, relative_sector_bbands_upper, relative_sector_bbands_middle, relative_sector_bbands_lower, relative_sector_bop,
                        relative_sector_cci, relative_sector_min, relative_sector_max, relative_sector_cmo, relative_sector_correl, relative_sector_dema, relative_sector_dx, relative_sector_sma_10, relative_sector_sma_20,
                        relative_sector_sma_50, relative_sector_sma_100, relative_sector_sma_150, relative_sector_sma_200,
                        relative_sector_ema, relative_sector_ema_10, relative_sector_ema_20, relative_sector_ema_50, relative_sector_ema_100, relative_sector_ema_150, relative_sector_ema_200, relative_sector_kama, relative_sector_ma,
                        relative_sector_mom, relative_sector_willr, relative_sector_sar, relative_sector_ultosc, relative_sector_tsf, relative_sector_natr, relative_sector_obv, 
                        relative_sector_mfi, relative_sector_tenkan_sen, relative_sector_kijun_sen, relative_sector_senkou_span_a, relative_sector_senkou_span_b, relative_sector_chikou_span, relative_sector_stdev, last_updated
                    ) VALUES (
                        %(symbol)s, %(index_name)s, %(date)s, %(open_price)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(sma_volume_10)s, %(roc)s, %(rsi)s, %(macd)s, %(macd_signal)s, %(macd_hist)s, %(ad)s,
                        %(adosc)s, %(adx)s, %(adxr)s, %(apo)s, %(aroon_up)s, %(aroon_down)s, %(aroonosc)s, %(atr)s, %(avgprice)s,
                        %(bbands_upper)s, %(bbands_middle)s, %(bbands_lower)s, %(bop)s, %(cci)s, %(min)s, %(max)s, %(cmo)s, %(correl)s,
                        %(dema)s, %(dx)s, %(ema)s, %(ema_10)s, %(ema_20)s, %(ema_50)s, %(ema_100)s, %(ema_150)s,  %(ema_200)s, %(kama)s, %(ma)s, %(mom)s, %(willr)s, %(sar)s,  %(ultosc)s, %(tsf)s, %(natr)s, %(obv)s, %(tenkan_sen)s, %(kijun_sen)s,
                        %(senkou_span_a)s, %(senkou_span_b)s, %(chikou_span)s, %(stdev)s, %(sma_10)s, %(sma_20)s, %(sma_50)s, %(sma_100)s, %(sma_150)s, 
                        %(sma_200)s, %(roc_sma_50)s, %(roc_sma_200)s, %(mfi)s, %(vwap)s, %(bbands_percent_b)s, %(prev_close)s, %(macd_prev)s, %(macd_hist_prev)s,%(macd_signal_prev)s, %(pct_diff_50d_sma)s, %(pct_diff_200d_sma)s, 
                        %(relative_close_spy)s, %(relative_price_rsi)s, %(relative_price_macd)s, %(relative_price_macd_signal)s, 
                        %(relative_price_macd_hist)s, %(relative_price_roc)s,  %(relative_price_ad)s,  %(relative_price_adosc)s,  %(relative_price_adx)s,  %(relative_price_adxr)s,  %(relative_price_apo)s,  %(relative_price_aroon_up)s, %(relative_price_aroon_down)s,  
                        %(relative_price_aroonosc)s, %(relative_price_atr)s, %(relative_price_avgprice)s, %(relative_price_bbands_upper)s, %(relative_price_bbands_middle)s, %(relative_price_bbands_lower)s, %(relative_price_bop)s, 
                        %(relative_price_cci)s, %(relative_price_min)s, %(relative_price_max)s, %(relative_price_cmo)s, %(relative_price_correl)s, %(relative_price_dema)s, %(relative_price_dx)s, %(relative_price_ema)s, 
                        %(relative_price_sma_10)s, %(relative_price_sma_20)s, %(relative_price_sma_50)s, %(relative_price_sma_100)s, %(relative_price_sma_150)s, %(relative_price_sma_200)s, 
                        %(relative_price_ema_10)s, %(relative_price_ema_20)s, %(relative_price_ema_50)s, %(relative_price_ema_100)s, %(relative_price_ema_150)s, %(relative_price_ema_200)s, 
                        %(relative_price_kama)s, %(relative_price_ma)s, %(relative_price_mom)s, %(relative_price_willr)s, %(relative_price_sar)s, %(relative_price_ultosc)s, %(relative_price_tsf)s, %(relative_price_natr)s, %(relative_price_obv)s,
                        %(relative_price_mfi)s, %(relative_price_tenkan_sen)s, %(relative_price_kijun_sen)s, %(relative_price_senkou_span_a)s, %(relative_price_senkou_span_b)s, %(relative_price_chikou_span)s, %(relative_price_stdev)s, 
                        %(relative_sector_close)s, %(relative_sector_open)s, %(relative_sector_high)s, %(relative_sector_low)s, %(relative_sector_volume)s, 
                        %(relative_sector_rsi)s,%(relative_sector_macd)s,%(relative_sector_macd_signal)s, %(relative_sector_macd_hist)s, %(relative_sector_roc)s,
                        %(relative_sector_ad)s, %(relative_sector_adosc)s, %(relative_sector_adx)s, %(relative_sector_adxr)s,
                        %(relative_sector_apo)s, %(relative_sector_aroon_up)s, %(relative_sector_aroon_down)s, %(relative_sector_aroonosc)s, %(relative_sector_atr)s, %(relative_sector_avgprice)s, %(relative_sector_bbands_upper)s, 
                        %(relative_sector_bbands_middle)s, %(relative_sector_bbands_lower)s, %(relative_sector_bop)s, %(relative_sector_cci)s, %(relative_sector_min)s, %(relative_sector_max)s,
                        %(relative_sector_cmo)s, %(relative_sector_correl)s, %(relative_sector_dema)s, %(relative_sector_dx)s, %(relative_sector_sma_10)s, %(relative_sector_sma_20)s, %(relative_sector_sma_50)s,
                        %(relative_sector_sma_100)s, %(relative_sector_sma_150)s, %(relative_sector_sma_200)s, %(relative_sector_ema)s, %(relative_sector_ema_10)s, %(relative_sector_ema_20)s, %(relative_sector_ema_50)s,
                        %(relative_sector_ema_100)s, %(relative_sector_ema_150)s, %(relative_sector_ema_200)s, %(relative_sector_kama)s, %(relative_sector_ma)s, %(relative_sector_mom)s, %(relative_sector_willr)s, %(relative_sector_sar)s, %(relative_sector_ultosc)s, %(relative_sector_tsf)s, %(relative_sector_natr)s, %(relative_sector_obv)s, %(relative_sector_mfi)s,
                        %(relative_sector_tenkan_sen)s, %(relative_sector_kijun_sen)s, %(relative_sector_senkou_span_a)s, %(relative_sector_senkou_span_b)s, %(relative_sector_chikou_span)s, %(relative_sector_stdev)s, NOW()
                    )
                """, row)
                conn.commit()

            time.sleep(0.2)  # Sleep to avoid rate limiting

    except Exception as e:
        print(f"Failed to fetch data for {symbol}: {e}")

cursor.close()
conn.close()

