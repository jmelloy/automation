import unittest
from unittest.mock import patch


class TestCompile(unittest.TestCase):


    def test_import_compile(self):
        """Dumb test to see if syntax check passes on everything, and make things show up in code coverage report"""

        import downloader
        import loader
        import queue_czds_downloads
