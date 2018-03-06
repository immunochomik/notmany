
from collections import namedtuple
from datetime import datetime
from unittest import TestCase

from notmany.store.base import BucketBase, StoreBase, FORMAT


def dt(date):
    return datetime.strptime(date, FORMAT)


class DummyBucket(BucketBase):
    def append(self, timestamp, data):
        pass

    def exists(self):
        return True

    def _create(self):
        pass

    def read(self):
        pass

    def delete(self):
        pass


class DummyStore(StoreBase):

    def _get_buckets(self, name, interval=None):
        pass

    def _create_bucket(self, name, start):
        return DummyBucket(name='bubu', start=start, length=self.bucket_size)

Check = namedtuple('Check', ['date', 'bucket'])


class BaseStoreGetBucketTestCase(TestCase):
    
    def test_get_bucket_with_size_less_than_hour_check_buckets(self):
        store = DummyStore(bucket_size=600)
        checks = (
            Check(date='2018-03-03 12:35:57', bucket=dt('2018-03-03 12:30:00')),
            Check(date='2018-03-03 12:05:42', bucket=dt('2018-03-03 12:00:00')),
            Check(date='2018-03-03 12:10:00', bucket=dt('2018-03-03 12:10:00')),
            Check(date='2018-03-03 12:12:57', bucket=dt('2018-03-03 12:10:00')),
            Check(date='2018-03-03 12:25:57', bucket=dt('2018-03-03 12:20:00')),
            Check(date='2018-03-03 12:33:57', bucket=dt('2018-03-03 12:30:00')),
            Check(date='2018-03-03 12:49:57', bucket=dt('2018-03-03 12:40:00')),
            Check(date='2018-03-03 23:51:57', bucket=dt('2018-03-03 23:50:00')),
            Check(date='2018-03-03 12:59:59', bucket=dt('2018-03-03 12:50:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', timestamp=check.date)
            self.assertEqual(bucket.start, check.bucket)

        store = DummyStore(bucket_size=300)
        checks = (
            Check(date='2018-03-03 12:35:57', bucket=dt('2018-03-03 12:35:00')),
            Check(date='2018-03-03 12:05:42', bucket=dt('2018-03-03 12:05:00')),
            Check(date='2018-03-03 12:10:00', bucket=dt('2018-03-03 12:10:00')),
            Check(date='2018-03-03 12:12:57', bucket=dt('2018-03-03 12:10:00')),
            Check(date='2018-03-03 12:25:57', bucket=dt('2018-03-03 12:25:00')),
            Check(date='2018-03-03 12:33:57', bucket=dt('2018-03-03 12:30:00')),
            Check(date='2018-03-03 12:49:57', bucket=dt('2018-03-03 12:45:00')),
            Check(date='2018-03-03 23:51:57', bucket=dt('2018-03-03 23:50:00')),
            Check(date='2018-03-03 12:59:59', bucket=dt('2018-03-03 12:55:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', timestamp=check.date)
            self.assertEqual(bucket.start, check.bucket)

        store = DummyStore(bucket_size=420)
        checks = (
            Check(date='2018-03-03 12:35:57', bucket=dt('2018-03-03 12:35:00')),
            Check(date='2018-03-03 12:05:42', bucket=dt('2018-03-03 12:00:00')),
            Check(date='2018-03-03 12:10:00', bucket=dt('2018-03-03 12:07:00')),
            Check(date='2018-03-03 12:12:57', bucket=dt('2018-03-03 12:07:00')),
            Check(date='2018-03-03 12:25:57', bucket=dt('2018-03-03 12:21:00')),
            Check(date='2018-03-03 12:33:57', bucket=dt('2018-03-03 12:28:00')),
            Check(date='2018-03-03 12:49:57', bucket=dt('2018-03-03 12:49:00')),
            Check(date='2018-03-03 23:51:57', bucket=dt('2018-03-03 23:49:00')),
            Check(date='2018-03-03 12:59:59', bucket=dt('2018-03-03 12:56:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', timestamp=check.date)
            self.assertEqual(bucket.start, check.bucket)
            self.assertEqual(bucket.length, store.bucket_size)

    def test_get_bucket_with_size_one_hour_check_buckets(self):
        store = DummyStore(bucket_size=3600)
        checks = (
            Check(date='2018-03-03 01:35:57', bucket=dt('2018-03-03 01:00:00')),
            Check(date='2018-03-03 04:05:42', bucket=dt('2018-03-03 04:00:00')),
            Check(date='2018-03-03 09:10:00', bucket=dt('2018-03-03 09:00:00')),
            Check(date='2018-03-03 12:12:57', bucket=dt('2018-03-03 12:00:00')),
            Check(date='2018-03-03 14:25:57', bucket=dt('2018-03-03 14:00:00')),
            Check(date='2018-03-03 16:33:57', bucket=dt('2018-03-03 16:00:00')),
            Check(date='2018-03-03 19:49:57', bucket=dt('2018-03-03 19:00:00')),
            Check(date='2018-03-03 21:51:57', bucket=dt('2018-03-03 21:00:00')),
            Check(date='2018-03-03 22:59:59', bucket=dt('2018-03-03 22:00:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', timestamp=check.date)
            self.assertEqual(bucket.start, check.bucket)
            self.assertEqual(bucket.length, store.bucket_size)
        
    def test_check_bucket_with_size_more_than_hour_check_buckets(self):
        store = DummyStore(bucket_size=7200)
        checks = (
            Check(date='2018-03-03 01:35:57', bucket=dt('2018-03-03 00:00:00')),
            Check(date='2018-03-03 04:05:42', bucket=dt('2018-03-03 04:00:00')),
            Check(date='2018-03-03 09:10:00', bucket=dt('2018-03-03 08:00:00')),
            Check(date='2018-03-03 12:12:57', bucket=dt('2018-03-03 12:00:00')),
            Check(date='2018-03-03 14:25:57', bucket=dt('2018-03-03 14:00:00')),
            Check(date='2018-03-03 16:33:57', bucket=dt('2018-03-03 16:00:00')),
            Check(date='2018-03-03 19:49:57', bucket=dt('2018-03-03 18:00:00')),
            Check(date='2018-03-03 21:51:57', bucket=dt('2018-03-03 20:00:00')),
            Check(date='2018-03-03 22:59:59', bucket=dt('2018-03-03 22:00:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', timestamp=check.date)
            self.assertEqual(bucket.start, check.bucket)
            self.assertEqual(bucket.length, store.bucket_size)

        store = DummyStore(bucket_size=3*3600)
        checks = (
            Check(date='2018-03-03 01:35:57', bucket=dt('2018-03-03 00:00:00')),
            Check(date='2018-03-03 04:05:42', bucket=dt('2018-03-03 03:00:00')),
            Check(date='2018-03-03 09:10:00', bucket=dt('2018-03-03 09:00:00')),
            Check(date='2018-03-03 12:12:57', bucket=dt('2018-03-03 12:00:00')),
            Check(date='2018-03-03 14:25:57', bucket=dt('2018-03-03 12:00:00')),
            Check(date='2018-03-03 16:33:57', bucket=dt('2018-03-03 15:00:00')),
            Check(date='2018-03-03 19:49:57', bucket=dt('2018-03-03 18:00:00')),
            Check(date='2018-03-03 21:51:57', bucket=dt('2018-03-03 21:00:00')),
            Check(date='2018-03-03 22:59:59', bucket=dt('2018-03-03 21:00:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', timestamp=check.date)
            self.assertEqual(bucket.start, check.bucket)
            self.assertEqual(bucket.length, store.bucket_size)

        store = DummyStore(bucket_size=24 * 3600)
        checks = (
            Check(date='2018-03-03 01:35:57', bucket=dt('2018-03-03 00:00:00')),
            Check(date='2018-03-03 04:05:42', bucket=dt('2018-03-03 00:00:00')),
            Check(date='2018-03-05 04:05:42', bucket=dt('2018-03-05 00:00:00')),
            Check(date='2018-02-07 04:05:42', bucket=dt('2018-02-07 00:00:00')),

        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', timestamp=check.date)
            self.assertEqual(bucket.start, check.bucket)
            self.assertEqual(bucket.length, store.bucket_size)
        
    def test_test_creation_of_bucket_more_than_24_h_rises(self):
        with self.assertRaises(NotImplementedError) as con:
            DummyStore(bucket_size=2*24*3600)
        self.assertEqual(str(con.exception), 'We do not support buckets larger than 24 h')


class BaseStoreTestCase(TestCase):


    def test_get_bucket_check_correct_bucket_returned(self):
        self.fail()

    def test_get_buckets_check_correct_buckets_returned(self):
        self.fail()

    def test_assignment_of_bucket_size_raises(self):
        self.fail()