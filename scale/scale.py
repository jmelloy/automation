import sys; sys.path.append("lib")

import boto3
import os

from clumpy.amazon.dynamodb import scan

aws_lambda = boto3.client("lambda", "us-west-2")


def handler(event, context):
    print(event)

    config = os.environ["config"]

    for item in scan(config):
        worker = item["key"]
        scale = item["scale"]

        for i in range(int(scale)):
            aws_lambda.invoke(FunctionName=worker,
                              InvocationType="Event")

            print(f"Started {scale} instances of {worker}")
