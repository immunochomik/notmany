from .base import StoreBase, BucketBase

import os


class Store(StoreBase):

    def __init__(self, directory):
        self.directory = directory

    def _get_bucket(self, name, timestamp):
        pass

    def _get_buckets(self, name, interval=None):
        pass

    def retrieve(self, name, interval):
        pass

    def forget(self, interval=None):
        pass


class Bucket(BucketBase):
    
    def append(self, timestamp, data):
        pass

    def exists(self):
        pass

    def _create(self):
        pass

    def read(self):
        pass

    def delete(self):
        pass