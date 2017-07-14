"""Handy methods to make things for testing."""

import datetime
import random
import sys
import time

import petname

DAY_SECONDS = 24 * 60 * 60


def random_integer(min=0, max=sys.maxsize):
    """ Get a random integer in the range [min, max]. """
    return random.randint(min, max)


def random_datetime(days_ago=3 * 365, days_from_now=0):
    """ Get a random datetime. By default gives a datetime in the last 3 years.
    Args:
      days_ago: Earliest number of days before now the random datetime will be.

      days_from_now: Most number of days in the future the random datetime will be.


    Returns:
        A randomly selected datetime in range [days_ago, days_from_now], relative to now().
    """
    return datetime.datetime.fromtimestamp(
        time.time() + random_integer(
            days_ago * -1 * DAY_SECONDS,
            days_from_now * DAY_SECONDS))


def random_noun():
    """ Get a random noun from the petname library. """
    return petname.Name()


def random_name():
    """ Get a random name from the petname library, in the form 'adjective-noun'. """
    return petname.Generate(2, '-')


def random_dict_list(min_length=0, max_length=10, min_width=0, max_width=10):
    """Makes a list of dicts of random data, each row having the same keyset and respective data types.

     The key names are randomly generated in 'adjective-noun' petname style. The value data types are randomly
     selected from [int, datetime, str].

    Args:
      min_length: minimum number of rows in the list, inclusive.

      max_length: maximum number of rows in the list, inclusive.

      min_width: minimum number of columns in each dict, inclusive.

      max_width: maximum number of columns in each dict, inclusive.


    Returns:
      A giant dict list.
    """
    generators = [random_integer, random_datetime, random_name]

    # set up row definition
    column_headers = [random_name() for _ in range(random.randint(min_width, max_width))]
    column_generators = [random.choice(generators) for _ in column_headers]

    return [
        {column_headers[i]: column_generators[i]() for i in range(0, len(column_headers))}
        for _ in range(0, random.randint(min_length, max_length))]
