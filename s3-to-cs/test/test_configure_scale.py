import unittest
from unittest.mock import patch
from moto import mock_dynamodb2
import boto3
import json
from test import FakeContext
import amazon

import configure_scale

message = {'AlarmName': 's3-to-bq-dev-s3tobqdevmessagesMessageAlarm1-TC53W0V9NFIT',
           'AlarmDescription': 'Alarm if queue contains more than 1 messages', 'AWSAccountId': '772688139689',
           'NewStateValue': 'ALARM',
           'NewStateReason': 'Threshold Crossed: 1 datapoint (2914.0) was greater than or equal to the threshold (1.0).',
           'StateChangeTime': '2017-06-23T20:53:50.494+0000', 'Region': 'US West - Oregon',
           'OldStateValue': 'INSUFFICIENT_DATA',
           'Trigger': {'MetricName': 'ApproximateNumberOfMessagesVisible', 'Namespace': 'AWS/SQS',
                       'StatisticType': 'Statistic', 'Statistic': 'SUM', 'Unit': None,
                       'Dimensions': [{'name': 'QueueName', 'value': 's3-to-bq-dev-messages'}], 'Period': 60,
                       'EvaluationPeriods': 1, 'ComparisonOperator': 'GreaterThanOrEqualToThreshold', 'Threshold': 1.0,
                       'TreatMissingData': '', 'EvaluateLowSampleCountPercentile': ''}}


class TestConfigureScale(unittest.TestCase):
    mock_dynamodb2 = mock_dynamodb2()

    def setUp(self):
        self.mock_dynamodb2.start()
        self.table_name = 'test-config'
        dynamodb = boto3.resource("dynamodb", "us-west-2")

        dynamodb.create_table(TableName=self.table_name,
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

    def tearDown(self):
        self.mock_dynamodb2.stop()

    def test_get_value(self):
        items = sorted(list(configure_scale.mapping.items()))

        for i, (start, val) in enumerate(items):
            if i + 1 < len(items):
                end = items[i + 1][0]
            else:
                end = 50000

            for check in range(start, end, (end - start) // 10):
                ret = configure_scale.get_value(check)
                self.assertEqual(ret, val, f'Expected {val}, got {ret} for {check}')

    def test_weird_input(self):

        with self.assertRaises(TypeError):
            configure_scale.get_value("lksjdf")

        ret = configure_scale.get_value(-1)
        self.assertEqual(ret, 0)

    def test_handler(self):
        with patch.dict('os.environ', {'config': self.table_name}):
            with open("test/configure_scale-alarm.json") as f:
                data = json.load(f)

            items = sorted(list(configure_scale.mapping.items()))

            for i, (start, val) in enumerate(items):
                if i + 1 < len(items):
                    end = items[i + 1][0]
                else:
                    end = 50000

                for check in range(start, end, (end - start) // 10):
                    message["NewStateValue"] = "ALARM"
                    message["NewStateReason"] = f'Threshold Crossed: 1 datapoint ({check}.0) was greater than ' \
                                                f'or equal to the threshold (1.0).'
                    data["Records"][0]["Sns"]["Message"] = json.dumps(message)

                    ret = configure_scale.handler(data, FakeContext())
                    self.assertEqual(ret, val, f'Expected {val}, got {ret} for {check}')

                    scale = amazon.get_scale(self.table_name)
                    self.assertEqual(scale, val, f'Expected dynamo to be {val}, got {scale} for {check}')


                message["NewStateValue"] = "OK"
                message["NewStateReason"] = f'Threshold Crossed: 1 datapoint (0.0) was less than ' \
                                            f'or equal to the threshold (1.0).'
                val = 0

                data["Records"][0]["Sns"]["Message"] = json.dumps(message)

                ret = configure_scale.handler(data, FakeContext())
                self.assertEqual(ret, val, f'Expected {val}, got {ret} for OK')

                scale = amazon.get_scale(self.table_name)
                self.assertEqual(scale, val, f'Expected dynamo to be {val}, got {scale} for OK')
