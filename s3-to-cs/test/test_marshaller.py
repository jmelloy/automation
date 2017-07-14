import unittest
from unittest.mock import patch
from moto import mock_s3, mock_sqs
import boto3
import json
from test import FakeContext
import amazon

import marshaller


class TestConfigureScale(unittest.TestCase):
    mock_s3 = mock_s3()
    mock_sqs = mock_sqs()

    def setUp(self):
        self.mock_s3.start()
        self.mock_sqs.start()

        self.queue = "test-marshaller"

        sqs = boto3.resource("sqs")
        sqs.create_queue(QueueName=self.queue)

    def tearDown(self):
        self.mock_sqs.stop()
        self.mock_s3.stop()

    def test_handler(self):
        with open("test/marshaller.json") as f:
            data = json.load(f)

        with patch.dict('os.environ', {"queue": self.queue}):
            marshaller.handler(data, FakeContext())
