import json
import os
import boto3
import datetime
from clumpy.google.bigquery import load
from copy import copy
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

"""Moves data from s3 to cloud storage"""

sqs = boto3.resource("sqs", "us-west-2")

def handler(event, context=None):
    logger.debug(event)

    ret = copy(event)

    parameters = event["queryStringParameters"]

    # These are required
    path = parameters.pop("path")
    fields = parameters.pop("fields")

    destination_table = parameters.pop("destination")
    destination_dataset = parameters.pop("datasetId")

    # Pop these because bq_load doesn't need them
    if "bucket" in parameters:
        parameters.pop("bucket")

    if "key" in parameters:
        parameters.pop("key")

    if "timestamp" in parameters:
        parameters.pop("timestamp")

    project = "whois-980"
    if "project" in parameters:
        project = parameters.pop("project")

    job_id = load(path, fields=fields, destination_table=destination_table,
                  destination_dataset=destination_dataset,
                  project_id=project,
                  **parameters)

    ret["job_id"] = job_id
    ret["timestamp"] = datetime.datetime.now().isoformat()

    logger.info(f"Started job {job_id} for {path}")

    r = None
    job_queue = os.environ.get("job_queue")
    if job_queue:
        jobs = sqs.get_queue_by_name(QueueName=job_queue)
        r = jobs.send_message(
            MessageBody=json.dumps(ret)
        )

    return job_id
