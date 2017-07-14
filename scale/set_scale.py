import sys; sys.path.append("lib")

import json
import os

from clumpy.amazon.dynamodb import set_scale

def handler(event, context):
    body = {
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "input": event
    }
    print(event)

    parameters = event["queryStringParameters"]
    table_name = os.environ.get("config")
    queue = parameters.get("queue")
    scale = parameters.get("value")

    set_scale(table_name=table_name, key=queue, scale=scale)

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response
