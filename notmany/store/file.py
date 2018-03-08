
import os
from tempfile import gettempdir

from .base import StoreBase, BucketBase, StoreSetupError


DEFAULT_DIR_NAME = 'notmany_store'


class Store(StoreBase):

    def __init__(self, directory=None, **kwargs):
        StoreBase.__init__(self, **kwargs)
        self.directory = directory
        self.set_up(directory)

    def set_up(self, directory):
        if directory is None:
            directory = os.path.join(gettempdir(), DEFAULT_DIR_NAME)
        self.directory = directory
        if not os.path.exists(directory):
            try:
                os.makedirs(directory, 0o655)
            except OSError as exc:
                raise StoreSetupError(str(exc))

    def _create_bucket(self, name, start):
        return Bucket()

    def _get_bucket(self, name, dt):
        pass

    def _get_buckets(self, name, interval=None):
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