from __future__ import absolute_import, division, print_function, unicode_literals
from datetime import datetime

import ciso8601

epoch = datetime.utcfromtimestamp(0)


def timedelta_to_micros(td):
    return (td.days * 86400 + td.seconds) * 10**6 + td.microseconds


def timedelta_to_nanos(td):
    return timedelta_to_micros(td) * 1000


def unix_time_nanos(dt):
    """Returns nanoseconds since epoch for a given datetime object"""
    return timedelta_to_nanos(dt - epoch)


def iso8601_to_nanos(date_str):
    """ Returns a nanoseconds since epoch for a given ISO-8601 date string

        Arguments:
        date: ISO-8601 UTC datetime string (e.g. "2016-01-01 12:00:00.000000")

        Return Values:
        int representing # of nanoseconds since "1970-01-01" for date
    """
    dt = ciso8601.parse_datetime(date_str)
    return unix_time_nanos(dt)


def iso8601_to_days_since_epoch(date_str):
    dt = ciso8601.parse_datetime(date_str)
    return (dt - epoch).days
