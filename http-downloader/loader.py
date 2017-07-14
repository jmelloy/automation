import json
import boto3
import datetime
import sys
import os
sys.path.append("lib")

import validators

sqs = boto3.client('sqs', "us-west-2")
cloudwatch = boto3.client("cloudwatch", "us-west-2")

function_name = __name__


def handler(event, context):
    """Main method for processing data.

    Checks URL for a URL, Bucket, and Key, and puts a message into S3 to process that data.
    event: Dictionary passed in from API Gateway, containing queryStringParameters and:
        url: URL to check
        bucket: S3 bucket to put results in
        key: Prefix for S3 files to go into
        file_name: filename within s3 prefix
        lua_script: extra parameters for lua script to run (see splash docs/consumer)
        proxy: which proxy agent to use
        user_agent: what user agent to use
        search_term: for registrar_scrapes, what term was used?

    context:
        AWS function to get time remaining, etc

    """

    service = os.environ.get("service", "http-downloader")

    print(event)

    body = {
        "input": event,
    }

    url = event["queryStringParameters"].get("url")
    bucket = event["queryStringParameters"].get("bucket")
    key = event["queryStringParameters"].get("key")

    if not bucket:
        body["message"] = "Missing required parameter: bucket"

    elif not key:
        body["message"] = "Missing required parameter: key"

    elif not url:
        body["message"] = "Missing required parameter: url"
    elif not validators.url(url):
        body["message"] = f"{url} is not a URL"

    if body.get("message"):
        return {
            "statusCode": 422,
            "body": json.dumps(body),

        }

    queue_name = os.environ["queue"]
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    status = "Failure"
    try:
        b = {"url": url,
             "bucket": bucket,
             "key": key}

        queue.send_message(
            MessageBody=json.dumps(b)
        )
        body["message"] = b

        response = {
            "statusCode": 200,
            "body": json.dumps(body)
        }

        status = "Success"

        return response
    finally:
        write_metrics(date_time=datetime.datetime.now(),
                      dim_value=status,
                      function_name=function_name,
                      namespace=service)


def write_metrics(date_time, dim_value, function_name, namespace):
    """
    This function writes out to CloudWatch Metrics for use in making dashboards to track success or failure of a run.
    :param date_time:  When was the task run?
    :param dim_value:  Was this 'Successful' or was it 'Failed'?
    :param function_name:  What's the name of this lambda task?
    :return: No return.
    """

    cloudwatch.put_metric_data(Namespace=namespace, MetricData=[
        {'MetricName': function_name,
         'Dimensions': [{'Name': 'Attempt', 'Value': dim_value}],
         'Timestamp': date_time, 'Value': 1}])

