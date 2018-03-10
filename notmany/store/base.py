from __future__ import print_function, division

import six

from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from functools import partial

__all__ = [
    'StoreBase',
    'BucketBase',
    'StoreSetupError',
    'Interval',
    'FORMAT',
    'get_datetime',
]

# bucket size in seconds
BUCKET_SIZE = 3600
SEC_IN_DAY = 3600 * 24

FORMAT = "%Y-%m-%d %H:%M:%S"

def dt(date):
    return datetime.strptime(date, FORMAT)


class Interval(object):
    __slots__ = ['start', 'end']

    def __init__(self, start, end=None, delta=None):
        """
        Create Interval object with datetime start and end
        :param start: the beginning
        :type start: datetime
        :param end: this is the end my only truly friend the end
        :type end: datetime | None
        :param delta: end can be expressed as timedelta
        :type delta: timedelta | None
        """
        self.start = get_datetime(start)

        if end is None:
            self.end = self.start + delta
        else:
            self.end = get_datetime(end)

        if self.start > self.end:
            raise RuntimeError('Invalid interval start after end')


def stamp(date):
    return datetime.strptime(date, FORMAT)


def get_datetime(timestamp):
    if type(timestamp) in [int, float]:
        return datetime.fromtimestamp(timestamp)
    if isinstance(timestamp, str):
        return datetime.strptime(timestamp, FORMAT)
    return timestamp


def naive_tstamp(dt):
    return (dt - datetime(1970, 1, 1)).total_seconds()


class StoreSetupError(Exception):
    pass


@six.add_metaclass(ABCMeta)
class StoreBase(object):

    def __init__(self, bucket_size=BUCKET_SIZE):
        if bucket_size % 60:
            raise NotImplementedError('Currently buckets have to be in minutes')
        if bucket_size > 3600 * 24:
            raise NotImplementedError('We do not support buckets larger than 24 h')
        self._bucket_size = bucket_size

    @property
    def bucket_size(self):
        return self._bucket_size

    def record(self, name, timestamp, data):
        """
        Record one data point in correct bucket
        :param name:
        :param timestamp:
        :param data:
        :return:
        """
        dt = get_datetime(timestamp)
        bucket = self._get_bucket(name, dt)
        bucket.append(naive_tstamp(dt), data)

    def retrieve(self, name, interval):
        """
        Retrieve all data points in given interval
        """
        for bucket in self._get_buckets(name, interval):
            for item in bucket:
                yield item

    def _get_bucket(self, name, dt):
        """
        Get one correct bucket for given interval
        :param name:
        :param dt: 
        :type dt: datetime
        :return:
        :rtype: BucketBase
        """

        base = dt.replace(minute=0, second=0, microsecond=0)

        if self._bucket_size < 3600:
            minutes_in_bucket = int(self._bucket_size / 60)
            return self._create_bucket(
                name=name,
                start=base.replace(
                    minute=int(dt.minute / minutes_in_bucket) * minutes_in_bucket))

        if self._bucket_size == 3600:
            return self._create_bucket(name=name, start=base)

        if self._bucket_size > 3600:
            h_in_bucket = int(self._bucket_size / 3600)
            return self._create_bucket(
                name=name,
                start=base.replace(hour=int(dt.hour / h_in_bucket) * h_in_bucket))

    @abstractmethod
    def _create_bucket(self, name, start):
        """
        Create a bucket instance appropriate to its concrete type
        :return:
        :rtype: BucketBase
        """
        pass

    def _get_buckets(self, name, interval):
        """
        Get buckets for name and interval
        :param name:
        :param interval:  time interval
        :type interval: Interval
        :return:
        :rtype: list of BucketBase
        """
        get_bucket = partial(self._get_bucket, name=name)

        bucket = get_bucket(dt=interval.start)

        while True:
            yield bucket
            bucket = get_bucket(dt=bucket.start + timedelta(seconds=self._bucket_size))
            if bucket.start > interval.end:
                break

    @abstractmethod
    def get_all(self, name):
        return []

    @abstractmethod
    def forget(self, name, interval=None):
        """
        Delete buckets in the interval
        :param name:
        :param interval: interval to delete
        :type interval: Interval
        :return:
        """




@six.add_metaclass(ABCMeta)
class BucketBase(object):
    __slots__ = ['name', 'start', 'length']

    def __init__(self, name, start, length):
        assert isinstance(start, datetime), 'Start is datetime'
        assert isinstance(length, int), 'Length is int'
        self.name = name
        self.start = start
        self.length = length

    def __iter__(self):
        return self.read()

    @abstractmethod
    def append(self, timestamp, data):
        pass

    @abstractmethod
    def read(self):
        for item in []:
            yield item

    @abstractmethod
    def delete(self):
        pass


