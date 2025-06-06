from movement_detecting import app as md_app
from api_polling import app as ap_app
from moto import mock_aws
import boto3
from datetime import datetime as dt
import os

@mock_aws
def test_detect_movement():
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

    # ensure that the function "fails" gracefully when there is less than 2 items in the table
    UTC_TIME_STR_1 = "2025-06-04T19:41:20Z"
    PRICE_1 = 141.90
    ap_app.write_price_to_table(UTC_TIME_STR_1, PRICE_1)
    detected, info = md_app.detect_movement(UTC_TIME_STR_1)
    assert(detected == False)
    assert (info == None)
    
    # ensure that a large enough movement is detected (and that return info is accurate)
    UTC_TIME_STR_2 = "2025-06-04T19:45:20Z"
    PRICE_2 = 141.20
    ap_app.write_price_to_table(UTC_TIME_STR_2, PRICE_2)
    detected, info = md_app.detect_movement(UTC_TIME_STR_2)
    assert(detected == True)
    assert(info[0] == "2025-06-04T19:41:20Z")
    assert(info[1] == "2025-06-04T19:45:20Z")
    assert(info[2] == 141.90)
    assert(info[3] == 141.20)
    assert(abs(info[4] - -0.70) < 0.001)
    
    # ensure that a movement too small is not detected
    UTC_TIME_STR_3 = "2025-06-04T19:50:20Z"
    PRICE_3 = 141.30
    ap_app.write_price_to_table(UTC_TIME_STR_3, PRICE_3)
    detected, info = md_app.detect_movement(UTC_TIME_STR_3)
    assert(detected == False)

    # ensure that a movement with an interval too large is not detected
    UTC_TIME_STR_4 = "2025-06-04T20:00:20Z"
    PRICE_4 = 141.80
    ap_app.write_price_to_table(UTC_TIME_STR_4, PRICE_4)
    detected, info = md_app.detect_movement(UTC_TIME_STR_4)
    assert(detected == False)

@mock_aws
def test_notify_of_movement():
    sns = boto3.client("sns", region_name="us-east-1")
    sns_topic_arn = sns.create_topic(Name="Topic")["TopicArn"]
    os.environ["SNS_TOPIC_ARN"] = sns_topic_arn
    info = ("2025-06-04T19:41:20Z", "2025-06-04T19:45:20Z", 141.70, 141.20, -0.50)
    message = md_app.notify_of_movement(info)
    assert(message == "NVDA Movement Detected: 2025-06-04T19:41:20Z -> 2025-06-04T19:45:20Z. $141.70 -> $141.20. -$0.50")