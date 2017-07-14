import os
import logging
from amazon import get_scale, set_scale, SNSNotification

logger = logging.getLogger()
logger.setLevel(logging.INFO)

mapping = {
    1: 1,
    50: 2,
    100: 4,
    1000: 8,
    5000: 20,
    20000: 50,
}


def get_value(queue_size: int):
    """Returns the biggest value in the mapping table less than queue_size input"""
    options = [0] + [value for check, value in mapping.items() if queue_size / check >= 1]
    return max(options)


def handler(event, context=None):
    """Sets the scale to the desired number based on the queue name & size"""
    logger.info(event)
    table_name = os.environ["config"]

    sns = SNSNotification(event)

    scale_value = 0
    if sns.NewStateValue == "ALARM":
        scale_value = get_value(sns.QueueSize)

    set_scale(table_name=table_name, key=sns.QueueName, scale=scale_value)

    logger.info(f"scale to {scale_value}")

    return scale_value
