import sys; sys.path.append("lib")
import requests
import os
import boto3
import logging
from urllib.parse import urlparse
import json
import datetime
import gzip
from io import BytesIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

"""Calls CZDS and adds to queue"""

sqs = boto3.resource("sqs", "us-west-2")

CDZAP_BASE_URL = "https://czdap.icann.org"
key = datetime.datetime.now().strftime("%Y-%m-%d")

def handler(event, context=None):
    logger.debug(event)

    token = os.environ["czds_api_token"]
    queue_name = os.environ["queue"]

    # Adapted from https://github.com/fourkitchens/czdap-tools
    r = requests.get(CDZAP_BASE_URL + '/user-zone-data-urls.json?token=' + token)
    if r.status_code != 200:
        raise Exception("Trouble connecting (HTTP returned: %d)" % r.status_code)

    urls = r.json()
    logger.info("Found %d files to download" % len(urls))

    queue = sqs.get_queue_by_name(QueueName=queue_name)

    for url in urls:
        parsed_url = urlparse(url)
        filename = os.path.basename(parsed_url.path) + ".txt.gz"

        r = requests.get(CDZAP_BASE_URL + url, stream=True)
        s = BytesIO(next(r.iter_content(1024)))
        try:
            with gzip.open(s) as gz:
                line = gz.readline()
                tld = line.decode("utf8").split("\t")[0]
        except Exception as e:
            print(e)
        if tld:
            filename = tld + filename

        print(url, filename)

        if queue:
            queue.send_message(
                MessageBody=json.dumps({"url": CDZAP_BASE_URL + url,
                                        "bucket": "co.donuts.dns.zone",
                                        "key": os.path.join(key, filename)})
            )

    return len(urls)

if __name__ == "__main__":
    handler({}, None)
