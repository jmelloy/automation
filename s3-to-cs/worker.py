import json
import boto3
import os
import datetime
import traceback
from google import cs_put_object, service
from bigquery import field_mapping
from amazon import dynamodb_put_item, invoke_lambda, write_metrics
import logging
from io import BytesIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

"""Moves data from s3 to cloud storage"""

sqs = boto3.resource("sqs", "us-west-2")
s3 = boto3.resource("s3")
error_table = os.environ.get("errors")
stage = os.environ.get("stage", "dev")

gs = service("storage", scope="devstorage.read_write")

if not error_table:
    logger.warning("No error table specified!")


def move_file(s3_key, s3_bucket, destination_bucket):
    st = datetime.datetime.now()

    obj = s3.Object(s3_bucket, s3_key)
    size = obj.content_length

    filename = "/tmp/" + os.path.split(s3_key)[-1]

    if size < 50 * 1024 * 1024:
        f = BytesIO()
        obj.download_fileobj(f)
        r = cs_put_object(f, bucket=destination_bucket, key=s3_key, check_size=size, gs=gs)
    else:
        obj.download_file(filename)
        with open(filename, "rb") as f:
            r = cs_put_object(f, bucket=destination_bucket, key=s3_key, check_size=size, gs=gs)
        os.remove(filename)

    end = datetime.datetime.now()

    write_metrics("Duration", {"QueueName": os.environ.get("queue", "test")},
                  value=(end - st).total_seconds(), unit="Seconds")
    return r


def handler(event, context):
    key = bucket = destination_bucket = None
    queue_name = os.environ["queue"]

    logger.info(f"Getting URL for {queue_name}")
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    bq_load_task = os.environ["bq_load"]

    success = errors = 0

    try:
        job_id = None
        while context.get_remaining_time_in_millis() > 15000:
            i = -1
            for i, message in enumerate(queue.receive_messages()):
                try:
                    msg = json.loads(message.body)

                    key = msg['key']
                    bucket = msg['bucket']
                    destination_bucket = msg["destination_bucket"]

                    logger.info(f'Working on {key} - {destination_bucket}')
                    if ".parsed." not in key:

                        move_file(s3_key=key, s3_bucket=bucket, destination_bucket=destination_bucket)

                        mapping = field_mapping(key=key, cs_bucket=destination_bucket, file_type="zone")

                        if mapping:
                            invoke_lambda(bq_load_task, mapping)

                    message.delete()
                    success += 1
                except Exception as e:
                    errors += 1
                    if error_table:
                        item = dict(timestamp=datetime.datetime.now().isoformat(),
                                    function_name=context.function_name,
                                    function_version=context.function_version,
                                    invoked_function_arn=context.invoked_function_arn,
                                    memory_limit_in_mb=context.memory_limit_in_mb,
                                    aws_request_id=context.aws_request_id,
                                    log_group_name=context.log_group_name,
                                    log_stream_name=context.log_stream_name,
                                    key=key, bucket=bucket, destination_bucket=destination_bucket)
                        item["exception"] = str(e)
                        item["stack_trace"] = traceback.format_exc()
                        dynamodb_put_item(item, error_table)
            if i == -1:
                break
    finally:
        write_metrics("MessagesProcessedSuccessfully", {"QueueName": os.environ.get("queue")}, success)
        write_metrics("MessagesProcessedWithErrors", {"QueueName": os.environ.get("queue")}, errors)
