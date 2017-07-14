import boto3
from marshaller import put_queue
import time
import json
import os

s3 = boto3.client("s3")
aws_lambda = boto3.client("lambda", "us-west-2")

worker = "sqs-worker-s3-to-cloud-storage-dev-dns_loader"

def get_prefix(bucket="co.donuts.dns.zone"):
    key = ""
    rs = {"IsTruncated": True}
    i = 0
    while rs["IsTruncated"]:
        rs = s3.list_objects(Bucket=bucket, Marker=key, Delimiter="/")
        for i, row in enumerate(rs.get("CommonPrefixes", [])):
            prefix = row["Prefix"]
            print(f"{prefix}")
            resp = aws_lambda.invoke(FunctionName=worker,
                              InvocationType="Event",
                              Payload=json.dumps({"prefix": prefix}))

            if i and i % 50 == 0:
                time.sleep(15)

def handler(event, context):
    prefix = event.get("prefix")

    for i, (key, size) in enumerate(list_bucket_s3(prefix=prefix, bucket="co.donuts.dns.zone")):
        print(f"Putting {key} / {size}")
        put_queue(key=key,
                  source_bucket="co.donuts.dns.zone", destination_bucket="donuts_zone")

    print(f'Placed {i + 1} items')

def list_bucket_s3(prefix, bucket="co.donuts.dns.ari"):

    key = ""
    rs = {"IsTruncated": True}
    i = 0
    while rs["IsTruncated"]:
        rs = s3.list_objects(Bucket=bucket, Prefix=prefix, Marker=key)
        if not rs.get("Contents"):
            return
        for row in rs.get("Contents", []):
            key = row["Key"]
            i += 1
            yield (row["Key"], row.get("Size",0),)
    print("%d rows returned for %s" % (i, prefix))

