import json
import boto3
import os
import logging
from io import BytesIO

import urllib.request

"""Checks status of BigQuery load job"""

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.resource("sqs", "us-west-2")
s3 = boto3.resource("s3")

def handler(event, context):
    queue_name = os.environ["queue"]
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    i = -1
    while context.get_remaining_time_in_millis() > 5000:
        for i, message in enumerate(queue.receive_messages(MaxNumberOfMessages=10)):
            msg = json.loads(message.body)

            url = msg["url"]
            with urllib.request.urlopen(url) as f:
                s3.Object(msg["bucket"], msg["key"]).put(Body=BytesIO(f.read()))

            message.delete()

    return i + 1

