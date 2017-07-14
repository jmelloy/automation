import unittest
import os

from apiclient.http import HttpMockSequence
from apiclient.discovery import build

from clumpy.google import bigquery, models
from clumpy.google.exceptions import BigQueryException


class TestBigQuery(unittest.TestCase):

    def setUp(self):
        loc = os.path.dirname(__file__)
        test_dir = os.path.join(loc, "google")
        with open(os.path.join(loc, "google", "bigquery-discovery.json")) as f:
            self.bigquery_discovery = f.read()

        with open(os.path.join(loc, "google", "bq_job_done.json")) as f:
            self.bigquery_done = f.read()

        with open(os.path.join(loc, "google", "bq_job_error.json")) as f:
            self.bigquery_error = f.read()

        with open(os.path.join(loc, "google", "bq_job_running.json")) as f:
            self.bigquery_running = f.read()

    def test_bq_load(self):
        """Tests whether BQ load processes data and returns a job ID"""

        # It's important to call the discovery method first, that gets passed into build()
        http = HttpMockSequence([
            ({'status': '200'}, self.bigquery_discovery),
            ({'status': '200'}, self.bigquery_running),
            ({'status': '200'}, self.bigquery_done),])

        gs = build("bigquery", "v2", http=http)

        job_id = bigquery.load("test.csv", destination_table="destination",
                               destination_dataset="ds",
                               fields=["asdf"], service=gs)
        self.assertEqual(job_id, "job_UmMf6X71NCgp4PxL1xE6WUfS8n0")

        bqj = models.BigQuery_Job(job_id, gs=gs)

        bqj.wait(timeout=.05)

    def test_bq_job_status_error(self):
        http = HttpMockSequence([
            ({'status': '200'}, self.bigquery_discovery),
            ({'status': '200'}, self.bigquery_error),
            ({'status': '200'}, self.bigquery_error)])

        gs = build("bigquery", "v2", http=http)

        resp = bigquery.job_status("job_UmMf6X71NCgp4PxL1xE6WUfS8n0", service=gs)

        bqj = models.BigQuery_Job(resp["jobReference"]["jobId"], gs=gs)

        self.assertEqual(bqj.state, "ERROR")
        self.assertIsNotNone(bqj.errors)
        self.assertIsNotNone(bqj.errorResult)

    def test_bq_wait(self):
        http = HttpMockSequence([
            ({'status': '200'}, self.bigquery_discovery),
            ({'status': '200'}, self.bigquery_running),
            ({'status': '200'}, self.bigquery_running),
            ({'status': '200'}, self.bigquery_running),
            ({'status': '200'}, self.bigquery_error)])

        gs = build("bigquery", "v2", http=http)

        resp = bigquery.job_status("job_UmMf6X71NCgp4PxL1xE6WUfS8n0", service=gs)

        bqj = models.BigQuery_Job(resp["jobReference"]["jobId"], gs=gs)

        with self.assertRaises(BigQueryException):
            bqj.wait(timeout=.05)

        self.assertEqual(bqj.state, "ERROR")
        self.assertIsNotNone(bqj.errors)
        self.assertIsNotNone(bqj.errorResult)

    def test_bq_job_status_done(self):
        http = HttpMockSequence([
            ({'status': '200'}, self.bigquery_discovery),
            ({'status': '200'}, self.bigquery_done),
            ({'status': '200'}, self.bigquery_done)])

        gs = build("bigquery", "v2", http=http)

        resp = bigquery.job_status("job_UmMf6X71NCgp4PxL1xE6WUfS8n0", service=gs)

        bqj = models.BigQuery_Job(resp["jobReference"]["jobId"], gs=gs)

        self.assertEqual(bqj.state, "DONE")
        self.assertIsNone(bqj.errors)
        self.assertIsNone(bqj.errorResult)

    def test_bad_data(self):
        http = HttpMockSequence([
            ({'status': '200'}, self.bigquery_discovery),])

        gs = build("bigquery", "v2", http=http)

        with self.assertRaises(BigQueryException):
            bigquery.load("gs://blah/blah", "blah", "blah", service=gs, laskjdf="lkjsdf")