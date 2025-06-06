import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime as dt
from datetime import timedelta as td
import os

MOVEMENT_THRESHOLD = 0.50 # $0.50
TIME_THRESHOLD = td(minutes=1, seconds=30)
STOCK = "NVDA"

def lambda_handler(event, context) -> None:
    est_time_str = event["Records"][0]["dynamodb"]["Keys"]["DateTime"]["S"]
    detected, info = detect_movement(est_time_str.replace(" ", "T") + "Z")
    print(detected, info)
    if detected:
        notify_of_movement(info)
    
def detect_movement(est_time_str) -> tuple[bool, tuple | None]:
    # retrieve the 2 most recent items in the table (by datetime)
    table = boto3.resource("dynamodb", region_name="us-east-1").Table("StockPriceTable")
    response = table.query(
        KeyConditionExpression=Key("Primary").eq("Primary") & Key("DateTime").lte(est_time_str),
        ScanIndexForward=False,
        Limit=2
    )

    # detect whether the magitude of movement is above the threshold
    items = [(dt.strptime(item["DateTime"], "%Y-%m-%dT%H:%M:%SZ"), float(item["Price"])) for item in response["Items"]]
    if len(items) != 2:
        return False, None
    earlyItem, lateItem = items[1], items[0]
    movement = lateItem[1] - earlyItem[1]
    movement_detected = True
    if lateItem[0] - earlyItem[0] > TIME_THRESHOLD: # account for cloudevent lag, yahoo finance history blips, and market open/close boundaries
        movement_detected = False
    if abs(movement) < MOVEMENT_THRESHOLD:
        movement_detected = False
    # (early_datetime, late_datetime, early_price, late_price, price_movement)
    return movement_detected, (earlyItem[0].strftime("%Y-%m-%dT%H:%M:%SZ"), lateItem[0].strftime("%Y-%m-%dT%H:%M:%SZ"), 
                               earlyItem[1], lateItem[1], movement)

def notify_of_movement(info) -> str:
    sns_topic_arn = os.environ.get("SNS_TOPIC_ARN")
    message = f"{STOCK} Movement Detected: {info[0]} -> {info[1]}. ${info[2]:.2f} -> ${info[3]:.2f}. {"-" if info[4] < 0 else "+"}${abs(info[4]):.2f}"
    boto3.client("sns", region_name="us-east-1").publish(
        TopicArn = sns_topic_arn,
        Message = message
    )
    return message