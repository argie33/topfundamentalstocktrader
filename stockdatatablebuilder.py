import pandas as pd
from sqlalchemy import create_engine, text

def connect_fetch():
    """ Connect to MySQL database, fetch data, write to new table using pandas, and calculate EV ratios and book value """
    # Connection string for SQLAlchemy engine
    engine = create_engine('mysql+mysqlconnector://user:pass@localhost:port/db', echo=False)

    # SQL JOIN query
    sql_query = """
        SELECT 
            skm.*,  -- Select all columns from stock_key_metrics
            sr.*,   -- Select all columns from stock_ratios
            cp.*,   -- Select all columns from company_profiles
            fg.*,   -- Select all columns from financial_growth
            iss.*,  -- Select all columns from income_statements
            bs.*    -- Select all columns from balance_sheets
        FROM 
            stock_key_metrics skm
        INNER JOIN 
            stock_ratios sr 
            ON skm.symbol = sr.symbol
        INNER JOIN 
            company_profiles cp 
            ON skm.symbol = cp.symbol
        INNER JOIN 
            financial_growth fg
            ON skm.symbol = fg.symbol
        INNER JOIN 
            income_statements iss
            ON skm.symbol = iss.symbol
        INNER JOIN 
            balance_sheets bs
            ON skm.symbol = bs.symbol;
    """

    # Load data into DataFrame
    df = pd.read_sql(sql_query, engine)

    # Print the first few rows to check the data
    print("Data loaded into DataFrame:")
    print(df.head())

    # Check if DataFrame is empty
    if df.empty:
        print("No data fetched. Exiting.")
        return

    # Calculate EV to EBITDA and EV to Revenue ratios
    df['EV_to_EBITDA'] = df['enterpriseValueTTM'] / df['ebitda']
    df['EV_to_Revenue'] = df['enterpriseValueTTM'] / df['revenue']

    # Calculate book value
    df['Book_Value'] = df['totalAssets'] - df['totalLiabilities']

    # Execute the DROP TABLE command using text() function for proper handling
    with engine.connect() as connection:
        connection.execute(text("DROP TABLE IF EXISTS stock_data;"))
        
        # Using pandas to create and insert data into new table
        df.to_sql(name='stock_data', con=connection, index=False, if_exists='replace')
        
        # Explicitly commit the transaction
        connection.commit()

    # Print number of rows inserted and completion message
    print(f"Data written to MySQL table 'stock_data'. Rows inserted: {len(df)}")
    print("EV/EBITDA, EV/Revenue, and Book Value calculations are added to the table.")

# Run the function to connect, fetch data, calculate ratios, and write to the database
connect_fetch()
