import unittest
from moto import mock_dynamodb2
import boto3
import amazon


class TestAmazon(unittest.TestCase):
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

    def test_set_value(self):
        ret = amazon.set_scale(50, self.table_name)

        with self.assertRaises(ValueError):
            ret = amazon.set_scale(50, None)

        with self.assertRaises(ValueError):
            ret = amazon.set_scale("asdf", self.table_name)

    def test_get_value(self):
        amazon.set_scale(50, self.table_name)
        ret = amazon.get_scale(self.table_name)
        self.assertEqual(50, ret)
