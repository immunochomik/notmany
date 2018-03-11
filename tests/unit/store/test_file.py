import os
import shutil
from datetime import timedelta, datetime
from tempfile import gettempdir
from unittest import TestCase
from uuid import uuid4

from notmany.store.file import Store, Bucket, DEFAULT_DIR_NAME
from notmany.store.base import StoreSetupError, Interval, RecordFew

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
                RecordFew('123456 cpu:7,some:8.4'),
                RecordFew('123457 cpu:8,some:8.4'),
                RecordFew('123456 cpu:8,some:8.4'),
            ], list(buck.read()))
        self.assertFalse(os.path.exists(buck.full_path))

    def test_bucket_iteration(self):
        with temporary_directory() as temp_dir:
            buck, _ = self.make_buck(base=temp_dir)
            self.assertFalse(os.path.exists(buck.full_path))
            buck.append(123456, 'cpu:7,some:8.4')
            buck.append(123457, 'cpu:8,some:8.4')
            self.assertEqual([
                RecordFew('123456 cpu:7,some:8.4'),
                RecordFew('123457 cpu:8,some:8.4'),
            ], list(buck))
        self.assertFalse(os.path.exists(buck.full_path))


    def test_delete_check_deleted(self):
        with temporary_directory() as temp_dir:
            buck, _ = self.make_buck(base=temp_dir)
            self.assertFalse(os.path.exists(buck.full_path))
            buck.append(123456, 'cpu:7,some:8.4')
            buck.append(123457, 'cpu:8,some:8.4')
            self.assertListEqual([
                RecordFew('123456 cpu:7,some:8.4'),
                RecordFew('123457 cpu:8,some:8.4'),
            ], list(buck))
            self.assertEqual('123456 cpu:7,some:8.4\n123457 cpu:8,some:8.4\n', file_content(buck.full_path))
            buck.delete()
            self.assertFalse(os.path.exists(buck.full_path))

    def test_create_from_file(self):
        base = os.path.join('tmp1')
        dire = os.path.join(base, 'some', '420', '2018_03_02')
        path = os.path.join(dire, '12_30_00')
        bucket = Bucket.create(base=base, file_path=path)
        self.assertEquals(bucket.name, 'some')
        self.assertEquals(bucket.length, 420)
        self.assertEquals(bucket.start, datetime(2018, 3, 2, 12, 30))
        self.assertEquals(bucket.dir, dire)
        self.assertEquals(bucket.file_name, '12_30_00')
        self.assertEquals(bucket.full_path, path)


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

    def test_forget_interval_including_all_check_namespace_deleted(self):
        with temporary_directory() as tem_dir:
            self.make_three_buckets(tem_dir=tem_dir)

            store = Store(directory=tem_dir, bucket_size=600)
            store.forget(name='some', interval=Interval(start=self.start, delta=timedelta(days=3)))
            self.assertEqual([], os.listdir(tem_dir))

    def test_forget_interval_with_not_all_in_it_check_we_leave_stuff_from_days_not_in_interval(self):
        with temporary_directory() as tem_dir:
            self.make_three_buckets(tem_dir=tem_dir)

            store = Store(directory=tem_dir, bucket_size=600)
            ts = self.start + timedelta(days=2)
            store.record(name='some', timestamp=ts, data='pending:1')
            ts += timedelta(seconds=1200)
            store.record(name='some', timestamp=ts, data='pending:2')
            ts += timedelta(hours=5)
            store.record(name='some', timestamp=ts, data='pending:3')
            base = os.path.join(tem_dir, 'some', '600', '2018_03_05')

            files = list(os.listdir(base))
            self.assertEqual(3, len(files))
            content = [file_content(base, file) for file in files]
            print(content)
            self.assertListEqual(
                content,
                ['1520272200.0 pending:3\n',
                 '1520253000.0 pending:1\n',
                 '1520254200.0 pending:2\n']
            )

            to_remove = Interval(start=self.start, delta=timedelta(days=1))
            store.forget(name='some', interval=to_remove)

            self.assertTrue(os.path.exists(base))

    def test_get_all_check_all(self):
        self.fail()

    def test_retrieve_with_interval_check_all_data_points_retrieved(self):

        with temporary_directory() as tem_dir:
            store = Store(directory=tem_dir, bucket_size=600)
            for i in range(20):
                store.record(name='foo', timestamp=self.start + timedelta(minutes=60 * i), data='pending:' + str(i))

            records = list(store.retrieve('foo', interval=Interval(start=self.start, delta=timedelta(hours=10))))

            self.assertEqual(len(records), 11)
            for item in records:
                print(item)
            self.fail()

    def test_record_metric_with_no_buckets_check_buckets_created(self):
        with temporary_directory() as tem_dir:
            self.make_three_buckets(tem_dir=tem_dir)

    def test_record_with_existing_buckets_check_correct_one_updated(self):
        with temporary_directory() as tem_dir:
            self.make_three_buckets(tem_dir=tem_dir)

            store = Store(directory=tem_dir, bucket_size=600)
            store.record(name='some',
                         timestamp=self.start + timedelta(seconds=20), data='pending:10;cpu:11.6')
            ts = self.start + timedelta(seconds=1220)
            store.record(name='some', timestamp=ts, data='pending:17;cpu:11.8')
            ts = ts + timedelta(hours=5)
            store.record(name='some', timestamp=ts, data='pending:4;cpu:11.8')
            base = os.path.join(tem_dir, 'some', '600', '2018_03_03')

            files = list(os.listdir(base))
            self.assertEqual(3, len(files))
            content = [file_content(base, file) for file in files]
            self.assertListEqual(
                content,
                ['1520099400.0 pending:3;cpu:11.8\n1520099420.0 pending:4;cpu:11.8\n',
                 '1520080200.0 pending:7;cpu:11.6\n1520080220.0 pending:10;cpu:11.6\n',
                 '1520081400.0 pending:7;cpu:11.8\n1520081420.0 pending:17;cpu:11.8\n']
            )


    def test_get_bucket_check_name(self):
        store = Store(bucket_size=24 * 3600)
        bucket = store._get_bucket('some_name', dt('2018-03-03 00:00:00'))
        self.assertEqual(bucket.name, 'some_name')

    def make_three_buckets(self, tem_dir):

        store = Store(directory=tem_dir, bucket_size=600)
        store.record(name='some', timestamp=self.start, data='pending:7;cpu:11.6')
        ts = self.start + timedelta(seconds=1200)
        store.record(name='some', timestamp=ts, data='pending:7;cpu:11.8')
        ts = ts + timedelta(hours=5)
        store.record(name='some', timestamp=ts, data='pending:3;cpu:11.8')
        base = os.path.join(tem_dir, 'some', '600', '2018_03_03')

        files = list(os.listdir(base))
        self.assertEqual(3, len(files))
        content = [file_content(base, file) for file in files]
        self.assertListEqual(
            content,
            ['1520099400.0 pending:3;cpu:11.8\n',
             '1520080200.0 pending:7;cpu:11.6\n',
             '1520081400.0 pending:7;cpu:11.8\n']
        )
