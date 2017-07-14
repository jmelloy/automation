import unittest
from unittest.mock import patch


class TestCompile(unittest.TestCase):


    def test_import_compile(self):
        """Dumb test to see if syntax check passes on everything, and make things show up in code coverage report"""

        import marshaller
        import configure_scale
        import scale
        import worker
        import load_bigquery
        import job_check
        import http_download
