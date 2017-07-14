import boto3
import json
import re
import datetime
import os
import logging
from botocore.client import ClientError

logger = logging.getLogger()

dynamodb = boto3.resource("dynamodb", "us-west-2")
aws_lambda = boto3.client("lambda", "us-west-2")
cw = boto3.client("cloudwatch", "us-west-2")


def set_scale(*, table_name: str, key: str, scale: int):
    if table_name is None:
        raise ValueError("Table name required")

    scale = int(scale)

    r = dynamodb_put_item({"key": key, "scale": scale}, table_name=table_name)

    if r.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) != 200:
        print(r)
        raise Exception("Setting scale failed!")
    return r


def get_scale(table_name: str, key: str):
    return int(dynamodb_get_item({"key": key}, table_name=table_name).get("scale", 0))


def dynamodb_put_item(item, table_name):
    table = dynamodb.Table(table_name)

    return table.put_item(
        Item=item
    )


def dynamodb_get_item(key, table_name):
    table = dynamodb.Table(table_name)

    return table.get_item(Key=key).get("Item", {})


def invoke_lambda(function, payload=None):
    d = dict(FunctionName=function,
             InvocationType='Event')

    if payload:
        d["Payload"] = json.dumps(payload)

    aws_lambda.invoke(**d)



def write_metrics(metric_name:str, dimensions: dict, value=1, namespace:str="BigQuery Load",
                  timestamp:datetime=None, unit:str=None):
    """
    This function writes out to CloudWatch Metrics for use in making dashboards to track success or failure of a run.
    metric_name: name of cloudwatch metric
    dimensions: dictionary of key/value pairs to include as dimensions
    value: value of metric
    namespace: service name

    :return: CW response
    """
    if timestamp is None:
        timestamp = datetime.datetime.now()

    if unit and unit not in (
            'Seconds' , 'Microseconds' , 'Milliseconds' ,
            'Bytes' , 'Kilobytes' , 'Megabytes' , 'Gigabytes' , 'Terabytes' , 'Bits' , 'Kilobits' ,
            'Megabits' , 'Gigabits' , 'Terabits' , 'Percent' , 'Count' , 'Bytes/Second' ,
            'Kilobytes/Second' , 'Megabytes/Second' , 'Gigabytes/Second' , 'Terabytes/Second' ,
            'Bits/Second' , 'Kilobits/Second' , 'Megabits/Second' , 'Gigabits/Second' , 'Terabits/Second' ,
            'Count/Second' , 'None'):
        raise Exception("Improper unit")

    return cw.put_metric_data(Namespace=namespace, MetricData=[
        {'MetricName': metric_name,
         'Dimensions': [{'Name': k, 'Value': v} for k, v in dimensions.items()],
         'Timestamp': timestamp, 'Value': value,
         "Unit": str(unit)}])


def upload_s3(filename, bucket, strip_path="", key=None, check_exists=True,
              delete=True, compress=False, raise_error=True):
    """ Uploads file to S3. If key is populated, sets key to that value. Else, uses filename.
    :param file: Absolute path of file to upload
    :param bucket: S3 Bucket to upload to
    :param strip_path: (Optional) Directories to strip from beginning of key.
    :param key: (Optional) sets key
    :param check_exists: Checks if file exists first
    :param delete: delete file after processing
    :param compress: gzip compress before uploading
    :param raise_error: Throw exception if file does not exist
    :return: Number of bytes uploaded
    """

    if not os.path.exists(filename):
        logger.warning("[upload_s3] %s does not exist", filename)
        if raise_error:
            raise IOError("No such file: %s" % filename)
        return

    if compress:
        logger.info("[upload_s3] Compressing %s" % (filename,))
        import gzip
        import shutil
        gzip_file = filename + ".gz"
        with open(filename, 'rb') as f_in, gzip.open(gzip_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        filename = gzip_file

    def percent_cb(complete):
        if complete:
            logger.debug("[upload_s3] %s [%d%% kB uploaded]" % (
                filename.replace(strip_path, ""), complete / 1024.0))

    logger.info("[upload_s3] Uploading %s to %s" % (filename, bucket))
    s3 = boto3.resource("s3", "us-west-2")

    key_name = key
    if not key:
        key_name = filename.replace(strip_path, "")

    if key_name.startswith("/"):
        key_name = key_name.strip("/")

    k = s3.Object(bucket, key_name)
    if check_exists:
        try:
            length = k.content_length
            if length == os.stat(filename).st_size:
                logger.info("[upload_s3] %s exists in %s" % (key_name, bucket))
                return
        except ClientError:
            pass

    try:
        k.upload_file(filename, Callback=percent_cb)
        if delete:
            os.remove(filename)
    except ClientError as e:
        logger.error("[upload_s3] Could not upload %s to %s - %s", filename, bucket, e)
