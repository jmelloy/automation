import json
import boto3
import os
import logging

import clumpy.google
from clumpy.google.models import BigQuery_Job
from clumpy.google.storage import delete_object
from clumpy.amazon.dynamodb import put_item
from clumpy.amazon.cloudwatch import write_metrics

import googleapiclient

"""Checks status of BigQuery load job"""

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs = boto3.resource("sqs", "us-west-2")
dynamodb = boto3.resource("dynamodb", "us-west-2")

cs = clumpy.google.service("storage", scope="devstorage.read_write")
bq = clumpy.google.service("bigquery", v="v2")

def handler(event, context):
    jobs = os.environ["jobs"]
    job_queue = sqs.get_queue_by_name(QueueName=jobs)

    i = -1
    success = errors = 0
    while context.get_remaining_time_in_millis() > 5000:
        for i, message in enumerate(job_queue.receive_messages(MaxNumberOfMessages=10)):
            msg = json.loads(message.body)

            successful_completion = process_message(msg, jobs)

            if successful_completion is True:
                message.delete()
                try:
                    delete_object(bucket=msg["bucket"], key=msg["key"], gs=cs)
                except googleapiclient.errors.HttpError as e:
                    print(e)
                success += 1
            elif successful_completion is False:
                errors += 1
                message.delete()

        if i == -1:
            break

    write_metrics("MessagesProcessedSuccessfully", {"QueueName": os.environ["jobs"]}, success)
    write_metrics("MessagesProcessedWithErrors", {"QueueName": os.environ["jobs"]}, errors)

    return i + 1


def process_message(msg, table_name=None, *, gs=None):
    if gs is None:
        gs = bq

    key = msg['key']
    bucket = msg['bucket']
    job_id = msg["job_id"]
    timestamp = msg.get("timestamp")
    logger.info(f'Checking {key} - {job_id}')
    bqj = BigQuery_Job(job_id, gs=gs)
    if bqj.state == "DONE":
        return True
    elif bqj.state == "ERROR":
        if table_name:
            put_item(dict(
                timestamp=timestamp or bqj.startTime,
                job_id=job_id,
                key=key,
                bucket=bucket,
                state=bqj.state,
                errors=bqj.errors,
                error=bqj.errorResult,
                start_time=bqj.startTime.isoformat(),
                end_time=bqj.endTime.isoformat()
            ), table_name=table_name)

        return False
    else:
        logger.info(f"{key} is still {bqj.state} ...")

