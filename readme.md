# Stock Analysis and Trading System

## Overview
This stock analysis and trading system is designed to generate rankings for the best stocks based on input data from a company's quarterly financial statements. It uses sophisticated algorithms to identify high-potential stocks based on a scoring system that evaluates financial health, growth prospects, and technical indicators, enabling active portfolio management by making informed trading decisions in real-time.  The project is also equipped to hold the top ranked stocks in a brokerage account.  

## Components
The project comprises several Python scripts, each serving specific functions:

### `balancesheet.py`
Processes and analyzes balance sheet data to assess the financial health of companies. It extracts key metrics like current ratio, debt-to-equity ratio, and other pertinent financial health indicators.

### `companyprofile.py`
Fetches and processes company demographic and financial information, providing a detailed profile that includes market capitalization, earnings per share, sector, and industry classifications.

### `incomestatement.py`
Analyzes income statements to evaluate profitability trends and revenue growth, extracting critical data such as gross profit margin, operating income, and net earnings.

### `loadexclusionlist.py`
Manages a list of stocks to be excluded from trading decisions, which could be based on various criteria such as historical underperformance, sectorial exposure, or legal constraints.

### `loadwashsale.py`
Handles the specifics of wash sale regulations, ensuring that the trading system complies with IRS rules by avoiding the repurchase of securities sold at a loss within a 30-day window.

### `main.py`
Acts as the orchestrator for the entire trading system, coordinating the workflow between data fetching, processing, and trading execution. It ensures that all components function seamlessly together.

### `stockdatatablebuilder.py`
Constructs and updates data tables essential for the analysis, including historical pricing, trading volumes, and technical indicators, facilitating quick access and manipulation.

### `stocklistload.py`
Responsible for loading and updating the list of stocks under consideration, ensuring the system has the most current data for screening and analysis.

### `stockscreener.py`
This script is crucial for identifying top-performing stocks. It applies complex algorithms to screen stocks based on comprehensive financial and technical criteria. The screener evaluates data from multiple sources to assign scores to each stock, highlighting those with the best growth potential and financial stability. It integrates outputs from other scripts like `balancesheet.py` and `incomestatement.py` to generate these scores. The top-scoring stocks are then passed to the trading module (`trade.py`) for action.

### `stocksfinancialgrowth.py`
Focuses on growth metrics, analyzing data points like year-over-year earnings growth, revenue growth, and projections to pinpoint stocks showing promising upward trajectories.

### `stocksratios.py`
Calculates a variety of financial ratios critical in the financial analysis, such as P/E ratio, ROE, ROA, and liquidity ratios, offering deeper insights into stock valuation and operational efficiency.

### `technicaldata.py`
Manages and analyzes technical trading indicators, like moving averages, RSI, and MACD, aiding in the identification of technical patterns that may signal buy or sell opportunities.

### `trade.py`
This script is responsible for the execution of trades. It uses the information provided by `stockscreener.py` about top-scoring stocks to execute trades. It manages both buy and sell orders based on real-time market conditions and predefined trading strategies. This script ensures that trading decisions are optimized for maximum return on investment, executing orders through the Alpaca API, with robust error handling and transaction logging for traceability.

## Running the Application
To operate the system:
1. Run `main.py` to initiate the data feeds, perform stock screening via `stockscreener.py`, and prepare the system for trading.
2. Execute `trade.py` after the screening process to make trades based on the identified top-scoring stocks.

## Requirements
- Python 3.8 or later
- Libraries: `pandas`, `numpy`, `sqlalchemy`, `requests`, `alpaca-trade-api`
- Alpaca API key for trading
- Financialmodelingprep API key for data feed
- MySQL database setup with necessary data structures

## Setup
1. Clone this repository to your machine.
2. Install the required Python libraries:
    pip install pandas numpy sqlalchemy requests alpaca-trade-api
3. Prepare your MySQL database according to the schema provided in the documentation.
4. Set your Alpaca API credentials in the environment variables or configuration files.

## Contributing
Contributions are welcome! Please fork the repository, make your proposed changes, and submit a pull request for review.
