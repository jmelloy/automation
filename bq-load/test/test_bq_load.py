import sys; sys.path.append("lib")
import unittest

import json
from copy import copy
from moto import mock_dynamodb2, mock_sqs, mock_cloudwatch
import boto3

import load_bigquery
from unittest.mock import patch
from clumpy.test_utils.aws_lambda import FakeContext

from apiclient.http import HttpMockSequence
from apiclient.discovery import build

class TestBQLoad(unittest.TestCase):
    mock_sqs = mock_sqs()
    mock_dynamodb2 = mock_dynamodb2()
    mock_cloudwatch = mock_cloudwatch()

    def setUp(self):
        self.queue = "test-queue"

        self.mock_sqs.start()
        self.mock_dynamodb2.start()
        self.mock_cloudwatch.start()

        sqs = boto3.resource("sqs", "us-west-2")
        sqs.create_queue(QueueName=self.queue)

        dynamodb = boto3.resource("dynamodb", "us-west-2")

        dynamodb.create_table(TableName=self.queue,
                              AttributeDefinitions=[
                                  {
                                      'AttributeName': 'key',
                                      'AttributeType': 'S'
                                  }

                              ],
                              KeySchema=[
                                  {
                                      'AttributeName': 'key',
                                      'KeyType': 'HASH'
                                  }
                              ],
                              ProvisionedThroughput={
                                  'ReadCapacityUnits': 123,
                                  'WriteCapacityUnits': 123
                              },
                              )
        with open("test/load_bigquery.json") as f:
            self.clean_data = json.load(f)

        with open("test/google/bigquery-discovery.json") as f:
            self.bigquery_discovery = f.read()

        with open("test/google/bq_job_running.json") as f:
            self.bigquery_running = f.read()

        with open("test/google/bq_job_done.json") as f:
            self.bigquery_done = f.read()


    def tearDown(self):
        self.mock_dynamodb2.stop()
        self.mock_sqs.stop()
        self.mock_cloudwatch.stop()

    def test_missing_path(self):
        data = copy(self.clean_data)
        data["queryStringParameters"].pop("path")
        with self.assertRaises(KeyError):
            ret = load_bigquery.handler(data, FakeContext())

    def test_missing_fields(self):
        data = copy(self.clean_data)
        data["queryStringParameters"].pop("fields")
        with self.assertRaises(KeyError):
            ret = load_bigquery.handler(data, FakeContext())

    def test_invalid_fields(self):
        data = copy(self.clean_data)
        data["queryStringParameters"]["fields"] = "klsjdf"

        with self.assertRaises(TypeError):
            ret = load_bigquery.handler(data, FakeContext())

    def test_missing_destination(self):
        data = copy(self.clean_data)
        data["queryStringParameters"].pop("destination")
        with self.assertRaises(KeyError):
            ret = load_bigquery.handler(data, FakeContext())

    def test_missing_dataset(self):
        data = copy(self.clean_data)
        data["queryStringParameters"].pop("datasetId")
        with self.assertRaises(KeyError):
            ret = load_bigquery.handler(data, FakeContext())

    def test_handler(self):
        http = HttpMockSequence([
            ({'status': '200'}, self.bigquery_discovery),
            ({'status': '200'}, self.bigquery_running),
            ({'status': '200'}, self.bigquery_done),])

        gs = build("bigquery", "v2", http=http)

        with patch("clumpy.google.service", gs):
            load_bigquery.handler(self.clean_data, FakeContext())
