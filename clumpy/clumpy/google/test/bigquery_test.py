import unittest

from util.google import bigquery


class BigQueryTest(unittest.TestCase):
    def test_table_list(self):
        """Test that a table exists and get the schema"""

        rs = list(bigquery.get_list_of_tables(datasetId='epp', projectId='whois-980'))

        self.assertGreater(len(rs), 0, "List does not contain any items")

        table = rs[0]

        schema = bigquery.get_table_schema(table_name=table, datasetId='epp', projectId='whois-980')

        self.assertGreater(len(schema), 0, "Schema does not contain any items")

        rec = schema[0]
        self.assertIn("name", rec, "Name is not in schema record")
        self.assertIn("type", rec, "Type is not in schema record")

