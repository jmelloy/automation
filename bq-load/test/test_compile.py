import unittest


class TestCompile(unittest.TestCase):

    def test_import_compile(self):
        """Dumb test to see if syntax check passes on everything, and make things show up in code coverage report"""

        import configure_scale
        import job_check
        import load_bigquery
