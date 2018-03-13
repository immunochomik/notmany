
import os
from datetime import datetime
from functools import partial
from tempfile import gettempdir

import shutil

import errno

from .base import StoreBase, BucketBase, StoreSetupError

DEFAULT_DIR_NAME = 'notmany_store'
CHUNK_SIZE = 65536
MAX_DELTA = 86400 * 7

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
                os.makedirs(directory)
            except OSError as exc:
                raise StoreSetupError(str(exc))

    def _create_bucket(self, name, start):
        return Bucket(name=name, start=start, length=self.bucket_size, base=self.directory)

    def get_all(self, name):
        """
        List directory and create buckets form files ?
        :param name:
        :return:
        """
        # for file in os walk yield bucket from file
        return []

    def forget(self, name, interval=None):
        """
        Will delete all day directories touched by the interval so be
        # TODO we should delte only buckets in interval if the
        # days in the interval are the same os we would not delte all day
        # heen just the individual buckets
        :param name:
        :param interval:
        :type interval: Interval
        :return: None
        """
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
                return

    def retrieve(self, name, interval=None):
        for bucket in self._get_buckets(name=name, interval=interval):
            for item in bucket:
                yield item

    def retrieve_raw(self, name, interval=None):
        for bucket in self._get_buckets(name=name, interval=interval):
            for item in bucket.raw():
                yield item


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
                    try:
                        yield self.line_to_record(line)
                    except (ValueError, IndexError) as exc:
                        print('Broken line {} {}'.format(line, exc))

    def raw(self, size=CHUNK_SIZE):
        if path_exists(self.full_path):
            with open(self.full_path, 'r') as fp:
                for chunk in iter(partial(fp.read, size), ''):
                    yield chunk

    @staticmethod
    def line_to_record(line):
        line = line.rstrip()
        parts = line.split(' ', 1)
        return float(parts[0]), parts[1]

    def delete(self):
        if path_exists(self.full_path):
            os.remove(self.full_path)

    @staticmethod
    def create(base, file_path):
        """
        Creates a Bucket instance from its file path
        :param base:
        :param file_path:
        :return:
        """
        description = file_path.replace(base, '').lstrip(os.sep)
        el = description.split(os.sep)

        start = datetime.strptime('{} {}'.format(el[2], el[3]), '%Y_%m_%d %H_%M_%S')

        return Bucket(name=el[0], start=start, length=int(el[1]), base=base)
