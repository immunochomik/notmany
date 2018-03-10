
import os
from tempfile import gettempdir

import shutil

import errno

from .base import StoreBase, BucketBase, StoreSetupError

DEFAULT_DIR_NAME = 'notmany_store'

# petty optimisations
path_exists = os.path.exists
path_join = os.path.join

#TODO add logging


class Store(StoreBase):
    def __init__(self, directory=None, **kwargs):
        StoreBase.__init__(self, **kwargs)
        self.directory = directory
        self.set_up(directory)

    def set_up(self, directory):
        if directory is None:
            directory = path_join(gettempdir(), DEFAULT_DIR_NAME)
        self.directory = directory
        if not path_exists(directory):
            try:
                os.makedirs(directory, 0o655)
            except OSError as exc:
                raise StoreSetupError(str(exc))

    def _create_bucket(self, name, start):
        return Bucket(name=name, start=start, length=self.bucket_size, base=self.directory)

    def get_all(self, name):
        return []

    def forget(self, name, interval=None):
        """
        Will delete all day directories touched by the interval so be
        :param name:
        :param interval:
        :return:
        """
        if interval is None:
            buckets = self.get_all(name)
        else:
            buckets = self._get_buckets(name=name, interval=interval)

        for directory in set([bucket.dir for bucket in buckets]):
            if os.path.exists(directory):
                shutil.rmtree(directory)

        parents = [
            path_join(self.directory, name, str(self.bucket_size)),
            path_join(self.directory, name)
        ]

        for directory in parents:
            try:
                os.rmdir(directory)
            except OSError as exc:
                if exc.errno != errno.ENOTEMPTY:
                    raise exc



class Bucket(BucketBase):
    __slots__ = ['dir', 'file_name']

    def __init__(self, name, start, length, base):
        BucketBase.__init__(self, name=name, start=start, length=length)
        self.file_name = self.start.strftime('%H_%M_%S')
        self.dir = path_join(base, name, str(self.length), self.start.strftime('%Y_%m_%d'))

    @property
    def full_path(self):
        return path_join(self.dir, self.file_name)

    def append(self, timestamp, data):
        if not path_exists(path_join(self.dir)):
            os.makedirs(self.dir)
        with open(self.full_path, 'a+') as fp:
            fp.write('{} {}\n'.format(timestamp, data))

    def read(self):
        if path_exists(self.full_path):
            with open(self.full_path, 'r') as fp:
                for line in fp:
                    yield line.rstrip()

    def delete(self):
        if path_exists(self.full_path):
            os.remove(self.full_path)
