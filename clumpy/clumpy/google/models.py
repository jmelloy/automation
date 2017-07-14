import datetime
import logging

import sys
import os
from clumpy.google.bigquery import job_status
from clumpy.google.exceptions import BigQueryException
sys.path.append("lib")


logger = logging.getLogger()

GOOGLEAPI_AUTH_URL = 'https://www.googleapis.com/auth/'

DEBUG = False
http_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "responses")
if DEBUG and not os.path.exists(http_path):
    os.makedirs(http_path)

def dparse(data, path):
    x = path.split("/")
    for k in x:
        if k:
            data = data.get(k, {})
    if data:
        return data
    return None


class BigQuery_Job(object):
    """Takes a BQ job id, calls the status API, and turns results into an object
    Ref: https://cloud.google.com/bigquery/docs/reference/v2/jobs#resource
    """

    def __init__(self, job_id, tags=None, projectId="whois-980", skip_check=False, gs=None):
        self.job_id = job_id
        self.tags = tags
        self.projectId = projectId
        self.complete = False
        self.gs = gs

        resp = None
        if not skip_check:
            resp = job_status(job_id, projectId=self.projectId, service=gs)
        self.raw_json = resp
        self.from_response(resp)

    def __repr__(self):
        return "<BQ Job %s: %s Started: %s Finished: %s%s>" % (self.job_id, self.state,
                                                               self.startTime, self.endTime,
                                                               ' (%s)' % self.tags if self.tags else '')

    def update(self):
        if not self.complete:
            logger.debug("Checking status for %s", self.job_id)
            resp = job_status(self.job_id, projectId=self.projectId, service=self.gs)
            self.from_response(resp)

    def wait(self, timeout=10, raise_exception=True):
        import time
        while not self.complete:
            self.update()
            logger.info("Status for %s (%s)", self.job_id, self.state)
            if self.complete:
                break
            time.sleep(timeout)

        if raise_exception and self.state == "ERROR":
            raise BigQueryException("BQ: %s" % self.errors)

    def from_response(self, resp):

        if resp is None:
            resp = {}

        self.raw_json = resp

        def p(path):
            return dparse(self.raw_json, path)

        self.kind = p("/kind")
        self.etag = p("/etag")
        self.projectId = p("/jobReference/projectId")
        self.jobId = p("/jobReference/jobId")
        self.selfLink = p("/selfLink")

        self.creationTime = _safe_datetime_from_ms(p("/statistics/creationTime"))
        self.startTime = _safe_datetime_from_ms(p("/statistics/startTime"))
        self.endTime = _safe_datetime_from_ms(p("/statistics/endTime"))

        self.id = p("/id")

        self.state = p("/status/state")

        if p("/status/errorResult"):
            self.state = 'ERROR'
            self.errors = ["%s (%s)" % (u['message'], u.get("location", "")) for u in p("/status/errors")]
            self.errorResult = p("/status/errorResult/message")
        else:
            self.errors = None
            self.errorResult = None

        self.job_type = p("/configuration.keys()[0]")

        # job_type == 'extract':
        self.destinationUri = p("/configuration/extract/destinationUri")

        # job_type == 'query':
        self.query = p("/configuration/query/query")
        self.destinationTableId = p("/configuration/query/destinationTable/tableId")
        self.destinationProjectId = p("/configuration/query/destinationTable/projectId")
        self.destinationDatasetId = p("/configuration/query/destinationTable/datasetId")

        self.inputFiles = _safe_int(p("/statistics/load/inputFiles"))
        self.inputBytes = _safe_int(p("/statistics/load/inputFileBytes"))
        self.outputRows = _safe_int(p("/statistics/load/outputRows"))

        self.user_email = p("/user_email")

        if self.state in ("DONE", "ERROR"):
            self.complete = True


def _safe_int(x):
    try:
        if x is not None:
            return int(x)
    except ValueError as ve:
        logger.warning("Could not convert %s to int", x)
        return x
    return x


def _safe_datetime_from_ms(x):
    try:
        if x is not None:
            x = _safe_int(x)
            return datetime.datetime.fromtimestamp(x / 1000)
        return None
    except Exception as e:
        logger.warning("Could not convert %s to date (%s)", x, e)
        return x