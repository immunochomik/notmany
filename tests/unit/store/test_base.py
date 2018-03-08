
from collections import namedtuple
from datetime import timedelta
from random import randint
from unittest import TestCase

from notmany.store.base import BucketBase, StoreBase, Interval
from tests.utils import dt


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

    def _create_bucket(self, name, start):
        return DummyBucket(name=name, start=start, length=self.bucket_size)


Check = namedtuple('Check', ['date', 'bucket'])


class BaseStoreGetBucketTestCase(TestCase):
    
    def test_get_bucket_with_size_less_than_hour_check_buckets(self):
        store = DummyStore(bucket_size=600)
        checks = (
            Check(date=dt('2018-03-03 12:35:57'), bucket=dt('2018-03-03 12:30:00')),
            Check(date=dt('2018-03-03 12:05:42'), bucket=dt('2018-03-03 12:00:00')),
            Check(date=dt('2018-03-03 12:10:00'), bucket=dt('2018-03-03 12:10:00')),
            Check(date=dt('2018-03-03 12:12:57'), bucket=dt('2018-03-03 12:10:00')),
            Check(date=dt('2018-03-03 12:25:57'), bucket=dt('2018-03-03 12:20:00')),
            Check(date=dt('2018-03-03 12:33:57'), bucket=dt('2018-03-03 12:30:00')),
            Check(date=dt('2018-03-03 12:49:57'), bucket=dt('2018-03-03 12:40:00')),
            Check(date=dt('2018-03-03 23:51:57'), bucket=dt('2018-03-03 23:50:00')),
            Check(date=dt('2018-03-03 12:59:59'), bucket=dt('2018-03-03 12:50:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', dt=check.date)
            self.assertEqual(bucket.start, check.bucket)


        store = DummyStore(bucket_size=300)
        checks = (
            Check(date=dt('2018-03-03 12:35:57'), bucket=dt('2018-03-03 12:35:00')),
            Check(date=dt('2018-03-03 12:05:42'), bucket=dt('2018-03-03 12:05:00')),
            Check(date=dt('2018-03-03 12:10:00'), bucket=dt('2018-03-03 12:10:00')),
            Check(date=dt('2018-03-03 12:12:57'), bucket=dt('2018-03-03 12:10:00')),
            Check(date=dt('2018-03-03 12:25:57'), bucket=dt('2018-03-03 12:25:00')),
            Check(date=dt('2018-03-03 12:33:57'), bucket=dt('2018-03-03 12:30:00')),
            Check(date=dt('2018-03-03 12:49:57'), bucket=dt('2018-03-03 12:45:00')),
            Check(date=dt('2018-03-03 23:51:57'), bucket=dt('2018-03-03 23:50:00')),
            Check(date=dt('2018-03-03 12:59:59'), bucket=dt('2018-03-03 12:55:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', dt=check.date)
            self.assertEqual(bucket.start, check.bucket)

        store = DummyStore(bucket_size=420)
        checks = (
            Check(date=dt('2018-03-03 12:35:57'), bucket=dt('2018-03-03 12:35:00')),
            Check(date=dt('2018-03-03 12:05:42'), bucket=dt('2018-03-03 12:00:00')),
            Check(date=dt('2018-03-03 12:10:00'), bucket=dt('2018-03-03 12:07:00')),
            Check(date=dt('2018-03-03 12:12:57'), bucket=dt('2018-03-03 12:07:00')),
            Check(date=dt('2018-03-03 12:25:57'), bucket=dt('2018-03-03 12:21:00')),
            Check(date=dt('2018-03-03 12:33:57'), bucket=dt('2018-03-03 12:28:00')),
            Check(date=dt('2018-03-03 12:49:57'), bucket=dt('2018-03-03 12:49:00')),
            Check(date=dt('2018-03-03 23:51:57'), bucket=dt('2018-03-03 23:49:00')),
            Check(date=dt('2018-03-03 12:59:59'), bucket=dt('2018-03-03 12:56:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', dt=check.date)
            self.assertEqual(bucket.start, check.bucket)
            self.assertEqual(bucket.length, store.bucket_size)

    def test_get_bucket_with_size_one_hour_check_buckets(self):
        store = DummyStore(bucket_size=3600)
        checks = (
            Check(date=dt('2018-03-03 01:35:57'), bucket=dt('2018-03-03 01:00:00')),
            Check(date=dt('2018-03-03 04:05:42'), bucket=dt('2018-03-03 04:00:00')),
            Check(date=dt('2018-03-03 09:10:00'), bucket=dt('2018-03-03 09:00:00')),
            Check(date=dt('2018-03-03 12:12:57'), bucket=dt('2018-03-03 12:00:00')),
            Check(date=dt('2018-03-03 14:25:57'), bucket=dt('2018-03-03 14:00:00')),
            Check(date=dt('2018-03-03 16:33:57'), bucket=dt('2018-03-03 16:00:00')),
            Check(date=dt('2018-03-03 19:49:57'), bucket=dt('2018-03-03 19:00:00')),
            Check(date=dt('2018-03-03 21:51:57'), bucket=dt('2018-03-03 21:00:00')),
            Check(date=dt('2018-03-03 22:59:59'), bucket=dt('2018-03-03 22:00:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', dt=check.date)
            self.assertEqual(bucket.start, check.bucket)
            self.assertEqual(bucket.length, store.bucket_size)
        
    def test_check_bucket_with_size_more_than_hour_check_buckets(self):
        store = DummyStore(bucket_size=7200)
        checks = (
            Check(date=dt('2018-03-03 01:35:57'), bucket=dt('2018-03-03 00:00:00')),
            Check(date=dt('2018-03-03 04:05:42'), bucket=dt('2018-03-03 04:00:00')),
            Check(date=dt('2018-03-03 09:10:00'), bucket=dt('2018-03-03 08:00:00')),
            Check(date=dt('2018-03-03 12:12:57'), bucket=dt('2018-03-03 12:00:00')),
            Check(date=dt('2018-03-03 14:25:57'), bucket=dt('2018-03-03 14:00:00')),
            Check(date=dt('2018-03-03 16:33:57'), bucket=dt('2018-03-03 16:00:00')),
            Check(date=dt('2018-03-03 19:49:57'), bucket=dt('2018-03-03 18:00:00')),
            Check(date=dt('2018-03-03 21:51:57'), bucket=dt('2018-03-03 20:00:00')),
            Check(date=dt('2018-03-03 22:59:59'), bucket=dt('2018-03-03 22:00:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', dt=check.date)
            self.assertEqual(bucket.start, check.bucket)
            self.assertEqual(bucket.length, store.bucket_size)

        store = DummyStore(bucket_size=3*3600)
        checks = (
            Check(date=dt('2018-03-03 01:35:57'), bucket=dt('2018-03-03 00:00:00')),
            Check(date=dt('2018-03-03 04:05:42'), bucket=dt('2018-03-03 03:00:00')),
            Check(date=dt('2018-03-03 09:10:00'), bucket=dt('2018-03-03 09:00:00')),
            Check(date=dt('2018-03-03 12:12:57'), bucket=dt('2018-03-03 12:00:00')),
            Check(date=dt('2018-03-03 14:25:57'), bucket=dt('2018-03-03 12:00:00')),
            Check(date=dt('2018-03-03 16:33:57'), bucket=dt('2018-03-03 15:00:00')),
            Check(date=dt('2018-03-03 19:49:57'), bucket=dt('2018-03-03 18:00:00')),
            Check(date=dt('2018-03-03 21:51:57'), bucket=dt('2018-03-03 21:00:00')),
            Check(date=dt('2018-03-03 22:59:59'), bucket=dt('2018-03-03 21:00:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', dt=check.date)
            self.assertEqual(bucket.start, check.bucket)
            self.assertEqual(bucket.length, store.bucket_size)

        store = DummyStore(bucket_size=24 * 3600)
        checks = (
            Check(date=dt('2018-03-03 01:35:57'), bucket=dt('2018-03-03 00:00:00')),
            Check(date=dt('2018-03-03 04:05:42'), bucket=dt('2018-03-03 00:00:00')),
            Check(date=dt('2018-03-05 04:05:42'), bucket=dt('2018-03-05 00:00:00')),
            Check(date=dt('2018-02-07 04:05:42'), bucket=dt('2018-02-07 00:00:00')),

        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', dt=check.date)
            self.assertEqual(bucket.start, check.bucket)
            self.assertEqual(bucket.length, store.bucket_size)
        
    def test_creation_of_bucket_more_than_24_h_rises(self):
        with self.assertRaises(NotImplementedError) as con:
            DummyStore(bucket_size=2*24*3600)
        self.assertEqual(str(con.exception), 'We do not support buckets larger than 24 h')

    def test_creation_of_bucket_with_size_not_minute_based_raises(self):
        with self.assertRaises(NotImplementedError) as con:
            DummyStore(bucket_size=200)
        self.assertEqual(str(con.exception), 'Currently buckets have to be in minutes')

class BaseStoreGetBucketsTestCase(TestCase):

    start = dt('2018-03-03 06:25:11')
    end = dt('2018-03-04 07:25:11')

    def test_get_buckets_with_size_less_than_hour_check_buckets(self):
        store = DummyStore(bucket_size=600)
        inter = Interval(start=self.start, delta=timedelta(hours=10))
        buckets = list(store._get_buckets('buba', interval=inter))

        self.assertEqual(buckets[0].start, dt('2018-03-03 06:20:00'))

        self.assertEqual(buckets[-1].start, dt('2018-03-03 16:20:00'))

        self.check_all_intervals(buckets, 'buba', timedelta(seconds=600), store)

    def test_get_buckets_300_check_buckets(self):
        store = DummyStore(bucket_size=300)
        inter = Interval(start=self.start, delta=timedelta(hours=5))
        buckets = list(store._get_buckets('buba', interval=inter))

        self.assertEqual(buckets[0].start, dt('2018-03-03 06:25:00'))

        self.assertEqual(buckets[-1].start, dt('2018-03-03 11:25:00'))

        self.check_all_intervals(buckets, 'buba', timedelta(seconds=300), store)


    def test_get_buckets_size_420_check_buckets(self):
        store = DummyStore(bucket_size=420)
        inter = Interval(start=self.start, delta=timedelta(hours=3))
        buckets = list(store._get_buckets('buba', interval=inter))

        self.assertEqual(buckets[0].start, dt('2018-03-03 06:21:00'))

        self.assertEqual(buckets[-1].start, dt('2018-03-03 09:21:00'))

        self.check_all_intervals(buckets, 'buba', timedelta(seconds=420), store)


    def test_get_bucket_with_size_hour_check_buckets(self):
        self.fail()

    def test_get_buckets_with_size_more_than_hour_check_buckets(self):
        self.fail()

    def check_all_intervals(self, buckets, name, delta, store):

        for i, bucket in enumerate(buckets):
            if i > 0:
                self.assertEqual(bucket.start, buckets[i - 1].start + delta)
            self.assertEqual(bucket.name, name)
            self.assertEqual(bucket.length, delta.seconds)

            # make sure that random timestamp from inside that bucket lands
            # in that bucket
            timestamp = bucket.start + timedelta(seconds=randint(1, bucket.length))
            same_bucket = store._get_bucket(name, timestamp)
            self.assertEqual(same_bucket.start, bucket.start)




class BaseStoreTestCase(TestCase):

    def test_assignment_of_bucket_size_raises(self):
        self.fail()