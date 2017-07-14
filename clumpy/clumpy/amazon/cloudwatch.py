import boto3
import datetime
import logging
logger = logging.getLogger()

cw = boto3.client("cloudwatch", "us-west-2")

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
