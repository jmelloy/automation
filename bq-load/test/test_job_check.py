import sys; sys.path.append("lib")

import unittest
from unittest.mock import patch
import json

from moto import mock_dynamodb2, mock_sqs, mock_cloudwatch
import boto3

from apiclient.http import HttpMockSequence
from apiclient.discovery import build

import job_check
from clumpy.test_utils.aws_lambda import FakeContext
from clumpy.amazon.dynamodb import get_item


class TestJobCheck(unittest.TestCase):
    mock_sqs = mock_sqs()
    mock_dynamodb2 = mock_dynamodb2()
    mock_cloudwatch = mock_cloudwatch()

    def setUp(self):
        self.queue_name = "test-queue"

        self.mock_sqs.start()
        self.mock_dynamodb2.start()
        self.mock_cloudwatch.start()

        sqs = boto3.resource("sqs", "us-west-2")
        sqs.create_queue(QueueName=self.queue_name)

        with open("test/google/bigquery-discovery.json") as f:
            self.bigquery_discovery = f.read()

        with open("test/google/bq_job_done.json") as f:
            self.bigquery_done = f.read()

        with open("test/google/bq_job_error.json") as f:
            self.bigquery_error = f.read()

        with open("test/google/bq_job_running.json") as f:
            self.bigquery_running = f.read()

        with open("test/google/storage_discovery.json") as f:
            self.storage_discovery = f.read()

        with open("test/job_check.json") as f:
            self.data = json.loads(f.read())

        dynamodb = boto3.resource("dynamodb", "us-west-2")

        dynamodb.create_table(TableName=self.queue_name,
                              AttributeDefinitions=[
                                  {
                                      'AttributeName': 'job_id',
                                      'AttributeType': 'S'
                                  }

                              ],
                              KeySchema=[
                                  {
                                      'AttributeName': 'job_id',
                                      'KeyType': 'HASH'
                                  }
                              ],
                              ProvisionedThroughput={
                                  'ReadCapacityUnits': 123,
                                  'WriteCapacityUnits': 123
                              },
                              )

    def tearDown(self):
        self.mock_dynamodb2.stop()
        self.mock_sqs.stop()
        self.mock_cloudwatch.stop()

    def test_process_message_done(self):
        """Tests whether process_message returns True for a completed job"""

        http = HttpMockSequence([
            ({'status': '200'}, self.bigquery_discovery),
            ({'status': '200'}, self.bigquery_done),])

        gs = build("bigquery", "v2", http=http)

        ret = job_check.process_message(self.data, gs=gs)
        self.assertEqual(ret, True)

    def test_process_message_error(self):
        """Tests whether process_message returns False for an errored job, and checks dynamodb for errors"""

        http = HttpMockSequence([
            ({'status': '200'}, self.bigquery_discovery),
            ({'status': '200'}, self.bigquery_error),])

        gs = build("bigquery", "v2", http=http)

        ret = job_check.process_message(self.data, self.queue_name, gs=gs)
        self.assertEqual(ret, False)

        r = get_item({"job_id": self.data["job_id"]}, self.queue_name)
        print(r)
        self.assertIsNotNone(r)

    def test_process_message_running(self):
        """Tests whether process_message returns None for a pending/running job"""

        http = HttpMockSequence([
            ({'status': '200'}, self.bigquery_discovery),
            ({'status': '200'}, self.bigquery_running),])

        gs = build("bigquery", "v2", http=http)

        ret = job_check.process_message(self.data, self.queue_name, gs=gs)
        self.assertIsNone(ret)

    def test_handler(self):
        sqs = boto3.resource("sqs", "us-west-2")

        job_queue = sqs.get_queue_by_name(QueueName=self.queue_name)
        job_queue.send_message(
            MessageBody=json.dumps(self.data)
        )

        bq_http = HttpMockSequence([
            ({'status': '200'}, self.bigquery_discovery),
            ({'status': '200'}, self.bigquery_done), ])

        cs_http = HttpMockSequence([
            ({'status': '200'}, self.storage_discovery),
            ({'status': '200'}, ''), ])

        def service(name, scope=None, v=None):
            print("In service!", name, scope, v)
            if name == "bigquery":
                return  build("bigquery", "v2", http=bq_http)
            elif name == "storage":
                return build("storage", "v1", http=cs_http)

        with patch.dict("os.environ", {"jobs": self.queue_name}):
            with patch("clumpy.google.service", service):
                job_check.handler({}, FakeContext(10))
