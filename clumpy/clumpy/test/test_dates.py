import unittest
from clumpy import date_math
import datetime

class TestDate(unittest.TestCase):

    def test_first_day(self):
        """Test generating first day of month"""
        dt = datetime.date(2015,6,5)
        val = date_math.first_day_of_month(dt)

        self.assertEqual(datetime.date(2015,6,1), val)

        dt = datetime.datetime(2015, 6, 5, 2, 2,3 , 2342)

        val = date_math.first_day_of_month(dt)

        self.assertEqual(datetime.datetime(2015,6,1), val)

        with self.assertRaises(ValueError) as ve:
            date_math.first_day_of_month(None)

        with self.assertRaises(ValueError) as ve:
            date_math.first_day_of_month(1255)

    def test_last_day(self):
        """Test generating last day of month"""
        dt = datetime.date(2016,2,1)
        val = date_math.last_day_of_month(dt)

        self.assertEqual(datetime.date(2016,2,29), val)

        dt = datetime.datetime(2015, 6, 5, 2, 2,3 , 2342)

        val = date_math.last_day_of_month(dt)

        self.assertEqual(datetime.datetime(2015,6,30), val)

        with self.assertRaises(ValueError) as ve:
            date_math.last_day_of_month(None)

        with self.assertRaises(ValueError) as ve:
            date_math.last_day_of_month(1255)

    def test_next_month(self):
        """Test first day of next month"""
        dt = datetime.date(2016, 2, 1)
        val = date_math.first_of_next_month(dt)

        self.assertEqual(datetime.date(2016, 3, 1), val)

        dt = datetime.datetime(2015, 6, 5, 2, 2, 3, 2342)

        val = date_math.first_of_next_month(dt)

        self.assertEqual(datetime.datetime(2015, 7, 1), val)

        with self.assertRaises(ValueError) as ve:
            date_math.last_day_of_month(None)

        with self.assertRaises(ValueError) as ve:
            date_math.last_day_of_month(1255)

    def test_iter_months(self):
        """Test monthly iteration"""

        start_date = datetime.datetime(2015,1,1)
        end_date = datetime.datetime(2016,1,1)

        val = date_math.iter_months(start_date, end_date)
        check = [datetime.datetime(2015, 1, 1, 0, 0),
                 datetime.datetime(2015, 2, 1, 0, 0),
                 datetime.datetime(2015, 3, 1, 0, 0),
                 datetime.datetime(2015, 4, 1, 0, 0),
                 datetime.datetime(2015, 5, 1, 0, 0),
                 datetime.datetime(2015, 6, 1, 0, 0),
                 datetime.datetime(2015, 7, 1, 0, 0),
                 datetime.datetime(2015, 8, 1, 0, 0),
                 datetime.datetime(2015, 9, 1, 0, 0),
                 datetime.datetime(2015, 10, 1, 0, 0),
                 datetime.datetime(2015, 11, 1, 0, 0),
                 datetime.datetime(2015, 12, 1, 0, 0)]

        self.assertEqual(list(val), check)

        val = date_math.iter_months(start_date, end_date, last_day=True)
        check = [datetime.datetime(2015, 1, 31, 0, 0),
                 datetime.datetime(2015, 2, 28, 0, 0),
                 datetime.datetime(2015, 3, 31, 0, 0),
                 datetime.datetime(2015, 4, 30, 0, 0),
                 datetime.datetime(2015, 5, 31, 0, 0),
                 datetime.datetime(2015, 6, 30, 0, 0),
                 datetime.datetime(2015, 7, 31, 0, 0),
                 datetime.datetime(2015, 8, 31, 0, 0),
                 datetime.datetime(2015, 9, 30, 0, 0),
                 datetime.datetime(2015, 10, 31, 0, 0),
                 datetime.datetime(2015, 11, 30, 0, 0),
                 datetime.datetime(2015, 12, 31, 0, 0)]

        self.assertEqual(list(val), check)
