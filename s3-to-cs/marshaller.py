import json
import boto3
import urllib.parse
import os

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


s3 = boto3.client('s3', "us-west-2")
sqs = boto3.client("sqs", "us-west-2")
queue_url = ""

destination = {
    "co.donuts.dns.zone": "donuts_zone"
}


def put_queue(key, source_bucket, destination_bucket, queue=None):
    if queue is None:
        queue = os.environ["queue"]
    global queue_url
    if not queue_url:
        logger.info(f"Getting URL for {queue}")
        queue_url = sqs.get_queue_url(
            QueueName=queue
        )["QueueUrl"]
        logger.debug(f"{queue_url}")

    message = dict(
            bucket=source_bucket, key=key, destination_bucket=destination_bucket
        )

    logger.debug(f"Sending {message} to {queue_url}")

    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message)
    )

    logger.debug(response)
    return response


def handler(event, context=None):
    logger.info(f'Starting marshaller with {len(event.get("Records", []))} record')
    logger.info(event)

    i = 0
    for i, record in enumerate(event["Records"]):
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        destination_bucket = destination[bucket]

        response = put_queue(key=key, source_bucket=bucket, destination_bucket=destination_bucket,
                             queue=os.environ["queue"])
        logger.debug(response)

    logger.info(f"Put {i + 1} messages")

