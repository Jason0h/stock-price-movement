import json
import yfinance as yf
from datetime import datetime as dt
from datetime import timedelta as td
from datetime import time
import pandas as pd
import boto3
from decimal import Decimal

STOCK = "NVDA"

def lambda_handler(event, context): 
    utc_time_str = json.loads(event["Records"][0]["body"])["message"]
    print("utc_time_str:", utc_time_str)
    stock_price = retrieve_price(utc_time_str)
    print("stock_price:", stock_price)
    if stock_price:
        write_price_to_table(utc_time_str, stock_price)
    
def retrieve_price(utc_time_str):
    # convert to est timezone and pandas timestamp format. then create +-30s offsets (for snapping to nearest full minute)
    utc_time_dt = dt.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%SZ")
    utc_time_ts = pd.Timestamp(utc_time_dt, tz="UTC")
    est_time_ts = utc_time_ts.tz_convert("America/New_York")
    start_time = est_time_ts - td(seconds=1000)
    end_time = est_time_ts + td(seconds=1000)
    print(start_time, end_time)
    
    # poll yahoo finance api and retrieve stock price at between start_time and end_time. only works within market hours
    est_time_only = est_time_ts.time()
    if time(9, 35, 0) <= est_time_only <= time(15, 55, 0):
        stock = yf.Ticker(STOCK)
        history = stock.history(start=est_time_ts.date(), end=est_time_ts.date() + td(days=1), interval="1m")
        try: 
            price = history.loc[(history.index >= start_time) & (history.index <= end_time)]["Close"].iloc[0]
        except: # records sometimes skip a minute. just pass in this case
            return None
        return price
    return None

def write_price_to_table(utc_time_str, stock_price):
    table = boto3.resource("dynamodb", region_name="us-east-1").Table("StockPriceTable")
    table.put_item(
        Item={
            "Primary": "Primary",
            "DateTime": utc_time_str,
            "Price": Decimal(str(stock_price))
        })