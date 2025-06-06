from api_polling import app
from moto import mock_aws
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime as dt

def test_retrieve_price():
    # given a UTC time, ensure that retrieve_price() fetches the correct approximate price
    UTC_TIME_STR_1 = "2025-06-04T19:41:20Z" # 3:41:20 PM EST
    PRICE_1 = 141.70 # rough price at 3:41:20 PM EST
    UTC_TIME_STR_2 = "2025-06-04T17:30:00Z" # 1:30:00 PM EST
    PRICE_2 = 140.77 # rough price at 1:30:00 PM EST
    UTC_TIME_STR_3 = "2025-06-04T15:30:00Z" # 11:30:00 PM EST
    PRICE_3 = 141.32 # rough price at 11:30:00 AM EST
    assert(abs(app.retrieve_price(UTC_TIME_STR_1) - PRICE_1) < 0.1)
    assert(abs(app.retrieve_price(UTC_TIME_STR_2) - PRICE_2) < 0.1)
    assert(abs(app.retrieve_price(UTC_TIME_STR_3) - PRICE_3) < 0.1)
    
    # ensure that the function "fails" gracefully when called on before/after market hours
    UTC_TIME_STR_4 = "2025-06-04T13:00:00Z" # 9:00:00 AM EST
    assert(app.retrieve_price(UTC_TIME_STR_4) == None)
    UTC_TIME_STR_5 = "2025-06-04T20:30:00Z" # 4:30:00 PM EST
    assert(app.retrieve_price(UTC_TIME_STR_5) == None)

@mock_aws
def test_write_price_to_table():
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    dynamodb.create_table(
        TableName="StockPriceTable",
        AttributeDefinitions=[
            {"AttributeName": "Primary", "AttributeType": "S"},
            {"AttributeName": "DateTime", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "Primary", "KeyType": "HASH"},
            {"AttributeName": "DateTime", "KeyType": "RANGE"}
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    
    # use write_price_to_table to add an item to a mock table & query for its existence
    UTC_TIME_STR_1 = "2025-06-04T19:41:20Z"
    PRICE_1 = 141.70
    app.write_price_to_table(UTC_TIME_STR_1, PRICE_1)
    table = dynamodb.Table("StockPriceTable")
    item = table.get_item(Key={"Primary": "Primary", "DateTime": UTC_TIME_STR_1})
    item_price = float(item["Item"]["Price"])
    assert(item_price == PRICE_1)
    
    # ensure that prices are ordered properly (by datetime)
    UTC_TIME_STR_2 = "2025-06-04T20:41:20Z"
    PRICE_2 = 141.70
    app.write_price_to_table(UTC_TIME_STR_2, PRICE_2)
    UTC_TIME_STR_3 = "2025-06-04T21:41:20Z"
    PRICE_3 = 141.70
    app.write_price_to_table(UTC_TIME_STR_3, PRICE_3)
    response = table.query(
        KeyConditionExpression=Key("Primary").eq("Primary"),
        ScanIndexForward=False,                  
    )
    dateTimes = [dt.strptime(item["DateTime"], "%Y-%m-%dT%H:%M:%SZ") for item in response["Items"]]
    for i in range(1, len(dateTimes)):
        assert(dateTimes[i-1] > dateTimes[i])