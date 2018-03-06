from __future__ import print_function, division

from datetime import datetime
from abc import ABC, abstractmethod

__all__ = [
    'StoreBase',
    'BucketBase',
    'StoreSetupError'
]

# bucket size in seconds
BUCKET_SIZE = 3600
SEC_IN_DAY = 3600 * 24

FORMAT = "%Y-%m-%d %H:%M:%S"


def stamp(date):
    return datetime.strptime(date, FORMAT)


def get_datetime(timestamp):
    if type(timestamp) in [int, float]:
        return datetime.fromtimestamp(timestamp)
    if isinstance(timestamp, str):
        return datetime.strptime(timestamp, FORMAT)
    return timestamp


class StoreSetupError(Exception):
    pass


class StoreBase(ABC):

    def __init__(self, bucket_size=BUCKET_SIZE):
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
        bucket = self._get_bucket(name, timestamp)
        bucket.append(timestamp, data)

    def retrieve(self, name, interval):
        """
        Retrieve all data points in given interval
        """
        for bucket in self._get_buckets(name, interval):
            for item in bucket:
                yield item

    def _get_bucket(self, name, timestamp):
        """
        Get one correct bucket for given interval
        :param name:
        :param timestamp:
        :return:
        :rtype: BucketBase
        """
        dt = get_datetime(timestamp)

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

    @abstractmethod
    def _get_buckets(self, name, interval=None):
        """
        Get buckets for name and interval
        :param name:
        :param interval:
        :return:
        :rtype: list of BucketBase
        """
        return []

    def forget(self, name, interval=None):
        """
        Delete buckets in the interval
        :param name:
        :param interval:
        :return:
        """
        for bucket in self._get_buckets(name=name, interval=interval):
            bucket.delete()


class BucketBase(ABC):
    __slots__ = ['name', 'start', 'length']

    def __init__(self, name, start, length):
        self.name = name
        self.start = start
        self.length = length
        if not self.exists():
            self._create()

    @abstractmethod
    def _create(self):
        pass

    @abstractmethod
    def exists(self):
        pass

    @abstractmethod
    def append(self, timestamp, data):
        pass

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def delete(self):
        pass


