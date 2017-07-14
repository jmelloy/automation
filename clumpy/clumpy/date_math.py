import datetime
from dateutil import parser

def first_day_of_month(dt: datetime.datetime):
    """Returns the first day of the month"""

    if type(dt) == datetime.date:
        return dt.replace(day=1)
    elif type(dt) == datetime.datetime:
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif type(dt) == str:
        d = parser.parse(dt)
        return d.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        raise ValueError("Not a date or datetime")


def last_day_of_month(dt: datetime.datetime):
    """Returns the last day of the month"""

    d = first_day_of_month(dt)
    d += datetime.timedelta(days=35)
    d = d.replace(day=1)
    return d - datetime.timedelta(days=1)


def first_of_next_month(dt: datetime.datetime):
    d = first_day_of_month(dt)
    d += datetime.timedelta(days=35)
    return d.replace(day=1)


def iter_months(start_date: datetime.datetime, end_date: datetime.datetime, last_day=False):
    """ Iterates through months, from start to end, returning the first of the month

    :param start_date: Day to start
    :param end_date: Day to end
    :param last: Return last day of month instead of first
    :return: Iterator<Date>
    """
    dt = start_date
    while dt < end_date:
        dt = first_day_of_month(dt)
        if last_day:
            dt = last_day_of_month(dt)

        yield dt
        dt = first_of_next_month(dt)


def iter_months_start_end(start_date: datetime.datetime, end_date: datetime.datetime):
    """
    Iterates through months, returning the first of that month and the first of the next month.
    :param start_date: start
    :param end_date: end
    :return:
    """

    dt = start_date
    while dt < end_date:
        st = first_day_of_month(dt)
        end = first_of_next_month(dt)
        yield (st, end)
        dt = first_of_next_month(dt)



TIME_DESCRIPTION_TABLE = (
    {"desc": "years", "unit": 60 * 60 * 24 * 365},
    {"desc": "months", "unit": 60 * 60 * 24 * 30},
    {"desc": "weeks", "unit": 60 * 60 * 24 * 7},
    {"desc": "days", "unit": 60 * 60 * 24},
    {"desc": "hours", "unit": 60 * 60},
    {"desc": "minutes", "unit": 60},
    {"desc": "seconds", "unit": 1}
)


def time_description(num_seconds):
    """Turns a number of seconds into a friendly description"""

    try:
        for item in TIME_DESCRIPTION_TABLE:
            result = float(num_seconds) / item["unit"]
            if result > 1:
                return "{result:.1f} {desc}".format(result=result, desc=item["desc"])
        return "Less than 1 second"
    except TypeError as inst:
        return str(inst)
