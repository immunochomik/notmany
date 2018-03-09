
import os
import shutil
from tempfile import gettempdir
from unittest import TestCase
from uuid import uuid4

from notmany.store.file import Store, Bucket, DEFAULT_DIR_NAME
from notmany.store.base import StoreSetupError


# TODO Add prevention form running this test as root as this will invalidate the test
# and can be done easily by mistake
from tests.utils import dt, temporary_directory





class FileBucketTestCase(TestCase):

    def setUp(self):
        self.name = 'some'
        self.start = dt('2018-03-03 12:30:00')

    def make_buck(self, base=None):
        if not base:
            base = os.path.join(gettempdir(), str(uuid4()))
        return Bucket(name='some', start=self.start, length=300, base=base), base

    def test_filename_creation(self):
        buck, base = self.make_buck()
        self.assertEqual(buck.full_path, os.path.join(base, 'some', '2018_03_03', '300', '12_30_00'))
        print(buck.full_path)

    def test_create_check_no_file_created(self):
        _, base = self.make_buck()
        self.assertFalse(os.path.exists(base))

    def test_append_with_no_file_check_created(self):
        with temporary_directory() as temp_dir:
            buck, _ = self.make_buck(base=temp_dir)
            self.assertFalse(os.path.exists(buck.full_path))
            buck.append(123456, 'cpu:7,some:8.4')
            self.assertTrue(os.path.exists(buck.full_path))
        self.assertFalse(os.path.exists(buck.full_path))


    def test_append_with_file_existing_check_appended(self):
        self.fail()

    def test_read_with_no_file_check_return_value(self):
        self.fail()

    def test_read_with_file_existing_check_return_value(self):
        self.fail()


class FileStoreTestCase(TestCase):

    def setUp(self):

        def delete_store_dir():
            if hasattr(self, 'store'):
                try:
                    shutil.rmtree(self.store.directory)
                except OSError:
                    # does not exist
                    pass

        self.addCleanup(delete_store_dir)

    def test_init_with_invalid_directory_check_rises(self):
        with self.assertRaises(StoreSetupError) as cont:
            Store('/some/invalid/path')
        self.assertTrue('[Errno 13] Permission denied' in str(cont.exception))

    def test_init_with_valid_dir_check_not_created(self):
        path = os.path.join(gettempdir(), str(uuid4()))
        self.assertFalse(os.path.exists(path))
        self.store = Store(path)
        self.assertTrue(os.path.exists(path))
        self.assertEqual(self.store.directory, path)

    def test_init_with_defaults_check_created(self):
        self.store = Store()
        self.assertTrue(os.path.exists(self.store.directory))

    def test_get_bucket_not_existing_check_return_and_not_created(self):
        self.fail()

    def test_get_bucket_existing_check_return(self):
        self.fail()

    def test_get_buckets_check_correct_buckets_returned(self):
        self.fail()

    def test_delete_buckets_check_correct_buckets_deleted(self):
        self.fail()

    def test_retrieve_with_interval_check_all_data_points_retrieved(self):
        self.fail()

    def test_record_metric_with_no_buckets_check_buckets_created(self):
        self.fail()

    def test_record_with_existing_buckets_check_correct_one_updated(self):
        self.fail()

    def test_get_bucket_check_name(self):
        store = Store(bucket_size=24 * 3600)
        bucket = store._get_bucket('some_name', dt('2018-03-03 00:00:00'))
        self.assertEqual(bucket.name, 'some_name')

