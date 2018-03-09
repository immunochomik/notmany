
import os
import shutil
from datetime import timedelta
from tempfile import gettempdir
from unittest import TestCase
from uuid import uuid4

from notmany.store.file import Store, Bucket, DEFAULT_DIR_NAME
from notmany.store.base import StoreSetupError, Interval

# TODO Add prevention form running this test as root as this will invalidate the test
# and can be done easily by mistake
from tests.utils import dt, temporary_directory, file_content


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
        self.assertEqual(buck.full_path, os.path.join(base, 'some', '300', '2018_03_03', '12_30_00'))
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
        with temporary_directory() as temp_dir:
            buck, _ = self.make_buck(base=temp_dir)
            self.assertFalse(os.path.exists(buck.full_path))
            buck.append(123456, 'cpu:7,some:8.4')
            self.assertEqual('123456 cpu:7,some:8.4\n', file_content(buck.full_path))
            buck.append(123457, 'cpu:8,some:8.4')
            self.assertEqual('123456 cpu:7,some:8.4\n123457 cpu:8,some:8.4\n', file_content(buck.full_path))

        self.assertFalse(os.path.exists(buck.full_path))

    def test_read_with_no_file_check_return_value(self):
        with temporary_directory() as temp_dir:
            buck, _ = self.make_buck(base=temp_dir)
            self.assertEqual([], list(buck.read()))

    def test_read_with_file_existing_check_return_value(self):
        with temporary_directory() as temp_dir:
            buck, _ = self.make_buck(base=temp_dir)
            self.assertFalse(os.path.exists(buck.full_path))
            buck.append(123456, 'cpu:7,some:8.4')
            buck.append(123457, 'cpu:8,some:8.4')
            buck.append(123456, 'cpu:8,some:8.4')
            self.assertEqual([
                '123456 cpu:7,some:8.4',
                '123457 cpu:8,some:8.4',
                '123456 cpu:8,some:8.4',
            ], list(buck.read()))
        self.assertFalse(os.path.exists(buck.full_path))

    def test_bucket_iteration(self):
        with temporary_directory() as temp_dir:
            buck, _ = self.make_buck(base=temp_dir)
            self.assertFalse(os.path.exists(buck.full_path))
            buck.append(123456, 'cpu:7,some:8.4')
            buck.append(123457, 'cpu:8,some:8.4')
            self.assertEqual([
                '123456 cpu:7,some:8.4',
                '123457 cpu:8,some:8.4',
            ], list(buck))
        self.assertFalse(os.path.exists(buck.full_path))

    def test_delete_check_deleted(self):
        with temporary_directory() as temp_dir:
            buck, _ = self.make_buck(base=temp_dir)
            self.assertFalse(os.path.exists(buck.full_path))
            buck.append(123456, 'cpu:7,some:8.4')
            buck.append(123457, 'cpu:8,some:8.4')
            self.assertEqual([
                '123456 cpu:7,some:8.4',
                '123457 cpu:8,some:8.4',
            ], list(buck))
            self.assertEqual('123456 cpu:7,some:8.4\n123457 cpu:8,some:8.4\n', file_content(buck.full_path))
            buck.delete()
            self.assertFalse(os.path.exists(buck.full_path))


class FileStoreTestCase(TestCase):

    def setUp(self):
        self.name = 'some'
        self.start = dt('2018-03-03 12:30:00')

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
        try:
            self.store = Store()
            self.assertTrue(os.path.exists(self.store.directory))
        finally:
            path = os.path.join(gettempdir(), DEFAULT_DIR_NAME)
            if os.path.exists(path):
                shutil.rmtree(path)

    def test_get_bucket_not_existing_check_return_and_not_created(self):
        with temporary_directory() as tem_dir:
            store = Store(directory=tem_dir, bucket_size=600)
            buck = store._get_bucket('some', dt('2017-03-03 10:25:11'))
            self.assertFalse(os.path.exists(buck.full_path))

    def test_delete_buckets_check_correct_buckets_deleted(self):
        self.fail()

    def test_retrieve_with_interval_check_all_data_points_retrieved(self):

        with temporary_directory() as tem_dir:
            store = Store(directory=tem_dir, bucket_size=600)
            store.record(name='some', timestamp=self.start, data='pending:7;cpu:11.6')
            ts = self.start + timedelta(seconds=1200)
            store.record(name='some', timestamp=ts, data='pending:7;cpu:11.8')

    def test_record_metric_with_no_buckets_check_buckets_created(self):
        with temporary_directory() as tem_dir:
            store = Store(directory=tem_dir, bucket_size=600)
            store.record(name='some', timestamp=self.start, data='pending:7;cpu:11.6')
            ts = self.start + timedelta(seconds=1200)
            store.record(name='some', timestamp=ts, data='pending:7;cpu:11.8')
            ts = ts + timedelta(hours=5)
            store.record(name='some', timestamp=ts, data='pending:3;cpu:11.8')
            files = list(os.listdir(os.path.join(tem_dir, 'some', '600', '2018_03_03')))
            self.assertEqual(3, len(files))



    def test_record_with_existing_buckets_check_correct_one_updated(self):
        self.fail()

    def test_get_bucket_check_name(self):
        store = Store(bucket_size=24 * 3600)
        bucket = store._get_bucket('some_name', dt('2018-03-03 00:00:00'))
        self.assertEqual(bucket.name, 'some_name')

