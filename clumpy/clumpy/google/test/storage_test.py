import io
import logging
import random
import unittest

import petname
from googleapiclient import http

from util.google import storage

TEST = 25
logging.addLevelName(TEST, 'TEST')
logging.basicConfig(level=TEST)
logger = logging.getLogger(__name__)

bucket = 'donuts-reporting'


class StorageTest(unittest.TestCase):
    """Storage tests."""

    def test_get_bucket_metadata(self):
        metadata = storage.get_bucket_metadata(bucket)
        del metadata['timeCreated']  # these change, don't compare
        del metadata['updated']  # these change, don't compare
        self.assertDictEqual(
            {'selfLink': 'https://www.googleapis.com/storage/v1/b/%s' % bucket,
             'storageClass': 'MULTI_REGIONAL', 'metageneration': '1',
             'projectNumber': '901762014784', 'kind': 'storage#bucket',
             'location': 'US', 'name': bucket,
             'id': bucket, 'etag': 'CAE='},
            metadata, "metadata from get_bucket_metadata doesn't match expected")

    def test_get_missing_object(self):
        filename = 'test/nonexistent_object.txt'
        # make sure we get a 404
        with self.assertRaisesRegex(http.HttpError, 'Not Found'):
            with io.BytesIO() as f:
                storage.get_object(bucket, filename, f)

    def test_delete_and_list_bucket(self):
        filename = 'test/test_delete_object.txt'

        # upload file and check response for correct size written
        data = petname.Generate(random.randint(1, 100), '-').encode()  # must encode to bytes!
        self.assertGreater(len(data), 0)  # make sure we're writing something!
        with io.BytesIO(data) as f:
            resp = storage.upload_object(bucket, filename, f)
            self.assertEqual(len(data), int(resp.get('size')))

        # after create, delete again and make sure it's gone
        resp = storage.delete_object(bucket, filename)
        self.assertEqual('', resp)
        objects = storage.list_bucket(bucket)
        filenames = [x['name'] for x in objects]
        self.assertNotIn(filename, filenames, 'delete did not remove the requested file')

        # make sure delete raises when deleting something that's not there
        with self.assertRaisesRegex(http.HttpError, 'Not Found'):
            storage.delete_object(bucket, filename)

    def test_upload_download_object(self):
        filename = 'test/test_upload_object.txt'

        # upload file and check response for correct size written
        data = petname.Generate(random.randint(1, 10000), '-').encode()  # must encode to bytes!
        self.assertGreater(len(data), 0)  # make sure we're writing something!
        with io.BytesIO(data) as f:
            resp = storage.upload_object(bucket, filename, f)
            self.assertEqual(len(data), int(resp.get('size')), 'upload failed')

        # download what we wrote and compare the bytes
        with io.BytesIO() as f:
            result = storage.get_object(bucket, filename, f)
            self.assertEqual(data, f.getvalue(), "downloaded data doesn't match uploaded data")
