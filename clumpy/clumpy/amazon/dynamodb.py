import boto3
import logging

logger = logging.getLogger()

dynamodb = boto3.resource("dynamodb", "us-west-2")

def set_scale(*, table_name: str, key: str, scale: int):
    if table_name is None:
        raise ValueError("Table name required")

    scale = int(scale)

    r = put_item({"key": key, "scale": scale}, table_name=table_name)

    if r.get("ResponseMetadata", {}).get("HTTPStatusCode", 0) != 200:
        print(r)
        raise Exception("Setting scale failed!")
    return r


def get_scale(table_name: str, key: str):
    return int(get_item({"key": key}, table_name=table_name).get("scale", 0))


def put_item(item, table_name):
    table = dynamodb.Table(table_name)

    return table.put_item(
        Item=item
    )


def get_item(key, table_name):
    table = dynamodb.Table(table_name)

    return table.get_item(Key=key).get("Item", {})


def scan(table_name):
    table = dynamodb.Table(table_name)

    return table.scan().get("Items", [])
