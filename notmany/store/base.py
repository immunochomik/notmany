

from abc import ABC, abstractmethod

__all__ = [
    'StoreBase',
    'BucketBase'
]


class StoreBase(ABC):

    def record(self, name, timestamp, data):
        """
        Record one data point in correct bucket
        :param name:
        :param timestamp:
        :param data:
        :return:
        """
        bucket = self._get_bucket(name, timestamp)
        bucket.append(name, data)

    @abstractmethod
    def retrieve(self, name, interval):
        """
        Retrieve all data points in given interval
        """
        pass

    @abstractmethod
    def _get_bucket(self, name, timestamp):
        """
        Get one correct bucket for given interval
        :param name:
        :param timestamp:
        :return:
        """
        pass

    @abstractmethod
    def _get_buckets(self, name, interval=None):
        pass

    @abstractmethod
    def forget(self, interval=None):
        """
        Delete buckets in the interval
        :param interval:
        :return:
        """
        pass


class BucketBase(ABC):
    __slots__ = ['name', 'start', 'length']

    def __init__(self, name, start, length):
        self.name = name
        self.start = start
        self.length = length

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


