import json

def lambda_handler(event, context): 
    for record in event['Records']:
        body = record['body']
        print("SQS Message Body:", body)
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
        }),
    }