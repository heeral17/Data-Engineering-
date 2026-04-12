import pandas as pd 
from sqlalchemy import create_engine
import schedule
import time

engine = create_engine('sqlite:///my_data.db')

def etl():
    print("Running ETL...")

    df = pd.read_csv('random_stock_market_dataset.csv')

    # Clean data BEFORE saving
    df = df.drop_duplicates()
    df = df.dropna()
    

    # Save cleaned data
    df.to_sql('df', engine, if_exists='append', index=False)

    print("ETL completed and data saved.")

def run_scheduler():
    # Schedule ETL at 10:30 daily
    schedule.every().minute.do(etl)

    while True:
        schedule.run_pending()
        time.sleep(60)

run_scheduler()
#whyyyyyyyyy
