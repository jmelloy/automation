import boto3
import json
import logging

logger = logging.getLogger()

aws_lambda = boto3.client("lambda", "us-west-2")


def invoke_lambda(function, payload=None):
    d = dict(FunctionName=function,
             InvocationType='Event')

    if payload:
        d["Payload"] = json.dumps(payload)

    return aws_lambda.invoke(**d)

