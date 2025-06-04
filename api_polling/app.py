import json
import yfinance as yf
from datetime import datetime as dt
from datetime import timedelta as td
import pandas as pd

STOCK = "NVDA"

def lambda_handler(event, context): 
    utc_time_str = json.loads(event["Records"][0]["body"])["message"]
    print("utc_time_str:", utc_time_str)
    stock_price = retrieve_price(utc_time_str)
    print("stock_price:", stock_price)
    
def retrieve_price(utc_time_str):
    # convert to est timezone and pandas timestamp format. then create +-30s offsets (for snapping to nearest full minute)
    utc_time_dt = dt.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%SZ")
    utc_time_ts = pd.Timestamp(utc_time_dt, tz="UTC")
    est_time_ts = utc_time_ts.tz_convert("America/New_York")
    start_time = est_time_ts - td(seconds=30)
    end_time = est_time_ts + td(seconds=30)
    # poll yahoo finance api and retrieve stock price at between start_time and end_time 
    stock = yf.Ticker(STOCK)
    history = stock.history(period="3d", interval="1m")
    price = history.loc[(history.index >= start_time) & (history.index <= end_time)]["Close"].iloc[0]
    return price
