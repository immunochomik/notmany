
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

    def forget(self, name, interval=None):
        pass

    def retrieve(self, name, interval=None):
        pass

    def get_all(self, name):
        pass

    def _create_bucket(self, name, start):
        return DummyBucket(name=name, start=start, length=self.bucket_size)


Check = namedtuple('Check', ['date', 'bucket'])


class BaseStoreGetBucketTestCase(TestCase):
    
    def test_get_bucket_with_size_less_than_hour_check_buckets(self):
        store = DummyStore(bucket_size=600)
        checks = (
            Check(date=dt('2018-03-03T12:35:57'), bucket=dt('2018-03-03T12:30:00')),
            Check(date=dt('2018-03-03T12:05:42'), bucket=dt('2018-03-03T12:00:00')),
            Check(date=dt('2018-03-03T12:10:00'), bucket=dt('2018-03-03T12:10:00')),
            Check(date=dt('2018-03-03T12:12:57'), bucket=dt('2018-03-03T12:10:00')),
            Check(date=dt('2018-03-03T12:25:57'), bucket=dt('2018-03-03T12:20:00')),
            Check(date=dt('2018-03-03T12:33:57'), bucket=dt('2018-03-03T12:30:00')),
            Check(date=dt('2018-03-03T12:49:57'), bucket=dt('2018-03-03T12:40:00')),
            Check(date=dt('2018-03-03T23:51:57'), bucket=dt('2018-03-03T23:50:00')),
            Check(date=dt('2018-03-03T12:59:59'), bucket=dt('2018-03-03T12:50:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', dt=check.date)
            self.assertEqual(bucket.start, check.bucket)

        store = DummyStore(bucket_size=300)
        checks = (
            Check(date=dt('2018-03-03T12:35:57'), bucket=dt('2018-03-03T12:35:00')),
            Check(date=dt('2018-03-03T12:05:42'), bucket=dt('2018-03-03T12:05:00')),
            Check(date=dt('2018-03-03T12:10:00'), bucket=dt('2018-03-03T12:10:00')),
            Check(date=dt('2018-03-03T12:12:57'), bucket=dt('2018-03-03T12:10:00')),
            Check(date=dt('2018-03-03T12:25:57'), bucket=dt('2018-03-03T12:25:00')),
            Check(date=dt('2018-03-03T12:33:57'), bucket=dt('2018-03-03T12:30:00')),
            Check(date=dt('2018-03-03T12:49:57'), bucket=dt('2018-03-03T12:45:00')),
            Check(date=dt('2018-03-03T23:51:57'), bucket=dt('2018-03-03T23:50:00')),
            Check(date=dt('2018-03-03T12:59:59'), bucket=dt('2018-03-03T12:55:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', dt=check.date)
            self.assertEqual(bucket.start, check.bucket)

        store = DummyStore(bucket_size=420)
        checks = (
            Check(date=dt('2018-03-03T12:35:57'), bucket=dt('2018-03-03T12:35:00')),
            Check(date=dt('2018-03-03T12:05:42'), bucket=dt('2018-03-03T12:00:00')),
            Check(date=dt('2018-03-03T12:10:00'), bucket=dt('2018-03-03T12:07:00')),
            Check(date=dt('2018-03-03T12:12:57'), bucket=dt('2018-03-03T12:07:00')),
            Check(date=dt('2018-03-03T12:25:57'), bucket=dt('2018-03-03T12:21:00')),
            Check(date=dt('2018-03-03T12:33:57'), bucket=dt('2018-03-03T12:28:00')),
            Check(date=dt('2018-03-03T12:49:57'), bucket=dt('2018-03-03T12:49:00')),
            Check(date=dt('2018-03-03T23:51:57'), bucket=dt('2018-03-03T23:49:00')),
            Check(date=dt('2018-03-03T12:59:59'), bucket=dt('2018-03-03T12:56:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', dt=check.date)
            self.assertEqual(bucket.start, check.bucket)
            self.assertEqual(bucket.length, store.bucket_size)

    def test_get_bucket_with_size_one_hour_check_buckets(self):
        store = DummyStore(bucket_size=3600)
        checks = (
            Check(date=dt('2018-03-03T01:35:57'), bucket=dt('2018-03-03T01:00:00')),
            Check(date=dt('2018-03-03T04:05:42'), bucket=dt('2018-03-03T04:00:00')),
            Check(date=dt('2018-03-03T09:10:00'), bucket=dt('2018-03-03T09:00:00')),
            Check(date=dt('2018-03-03T12:12:57'), bucket=dt('2018-03-03T12:00:00')),
            Check(date=dt('2018-03-03T14:25:57'), bucket=dt('2018-03-03T14:00:00')),
            Check(date=dt('2018-03-03T16:33:57'), bucket=dt('2018-03-03T16:00:00')),
            Check(date=dt('2018-03-03T19:49:57'), bucket=dt('2018-03-03T19:00:00')),
            Check(date=dt('2018-03-03T21:51:57'), bucket=dt('2018-03-03T21:00:00')),
            Check(date=dt('2018-03-03T22:59:59'), bucket=dt('2018-03-03T22:00:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', dt=check.date)
            self.assertEqual(bucket.start, check.bucket)
            self.assertEqual(bucket.length, store.bucket_size)
        
    def test_check_bucket_with_size_more_than_hour_check_buckets(self):
        store = DummyStore(bucket_size=7200)
        checks = (
            Check(date=dt('2018-03-03T01:35:57'), bucket=dt('2018-03-03T00:00:00')),
            Check(date=dt('2018-03-03T04:05:42'), bucket=dt('2018-03-03T04:00:00')),
            Check(date=dt('2018-03-03T09:10:00'), bucket=dt('2018-03-03T08:00:00')),
            Check(date=dt('2018-03-03T12:12:57'), bucket=dt('2018-03-03T12:00:00')),
            Check(date=dt('2018-03-03T14:25:57'), bucket=dt('2018-03-03T14:00:00')),
            Check(date=dt('2018-03-03T16:33:57'), bucket=dt('2018-03-03T16:00:00')),
            Check(date=dt('2018-03-03T19:49:57'), bucket=dt('2018-03-03T18:00:00')),
            Check(date=dt('2018-03-03T21:51:57'), bucket=dt('2018-03-03T20:00:00')),
            Check(date=dt('2018-03-03T22:59:59'), bucket=dt('2018-03-03T22:00:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', dt=check.date)
            self.assertEqual(bucket.start, check.bucket)
            self.assertEqual(bucket.length, store.bucket_size)

        store = DummyStore(bucket_size=3*3600)
        checks = (
            Check(date=dt('2018-03-03T01:35:57'), bucket=dt('2018-03-03T00:00:00')),
            Check(date=dt('2018-03-03T04:05:42'), bucket=dt('2018-03-03T03:00:00')),
            Check(date=dt('2018-03-03T09:10:00'), bucket=dt('2018-03-03T09:00:00')),
            Check(date=dt('2018-03-03T12:12:57'), bucket=dt('2018-03-03T12:00:00')),
            Check(date=dt('2018-03-03T14:25:57'), bucket=dt('2018-03-03T12:00:00')),
            Check(date=dt('2018-03-03T16:33:57'), bucket=dt('2018-03-03T15:00:00')),
            Check(date=dt('2018-03-03T19:49:57'), bucket=dt('2018-03-03T18:00:00')),
            Check(date=dt('2018-03-03T21:51:57'), bucket=dt('2018-03-03T21:00:00')),
            Check(date=dt('2018-03-03T22:59:59'), bucket=dt('2018-03-03T21:00:00')),
        )
        for check in checks:
            bucket = store._get_bucket(name='fufu', dt=check.date)
            self.assertEqual(bucket.start, check.bucket)
            self.assertEqual(bucket.length, store.bucket_size)

        store = DummyStore(bucket_size=24 * 3600)
        checks = (
            Check(date=dt('2018-03-03T01:35:57'), bucket=dt('2018-03-03T00:00:00')),
            Check(date=dt('2018-03-03T04:05:42'), bucket=dt('2018-03-03T00:00:00')),
            Check(date=dt('2018-03-05T04:05:42'), bucket=dt('2018-03-05T00:00:00')),
            Check(date=dt('2018-02-07T04:05:42'), bucket=dt('2018-02-07T00:00:00')),

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

    start = dt('2018-03-03T06:25:11')
    end = dt('2018-03-04T07:25:11')

    def test_get_buckets_with_size_less_than_hour_check_buckets(self):
        store = DummyStore(bucket_size=600)
        inter = Interval(start=self.start, delta=timedelta(hours=10))
        buckets = list(store._interval_buckets('buba', interval=inter))

        self.assertEqual(buckets[0].start, dt('2018-03-03T06:20:00'))

        self.assertEqual(buckets[-1].start, dt('2018-03-03T16:20:00'))

        self.check_all_intervals(buckets, 'buba', timedelta(seconds=600), store)

    def test_get_buckets_300_check_buckets(self):
        store = DummyStore(bucket_size=300)
        inter = Interval(start=self.start, delta=timedelta(hours=5))
        buckets = list(store._interval_buckets('buba', interval=inter))

        self.assertEqual(buckets[0].start, dt('2018-03-03T06:25:00'))

        self.assertEqual(buckets[-1].start, dt('2018-03-03T11:25:00'))

        self.check_all_intervals(buckets, 'buba', timedelta(seconds=300), store)

    def test_get_buckets_size_420_check_buckets(self):
        store = DummyStore(bucket_size=420)
        inter = Interval(start=self.start, delta=timedelta(hours=27))
        buckets = list(store._interval_buckets('buba', interval=inter))

        self.assertEqual(buckets[0].start, dt('2018-03-03T06:21:00'))

        self.assertEqual(buckets[-1].start, dt('2018-03-04T09:21:00'))

        self.check_all_intervals(buckets, 'buba', timedelta(seconds=420), store)

    def test_get_bucket_with_1_size_hour_check_buckets(self):
        store = DummyStore(bucket_size=3600)
        inter = Interval(start=self.start, delta=timedelta(hours=27))
        buckets = list(store._interval_buckets('buba', interval=inter))

        self.assertEqual(buckets[0].start, dt('2018-03-03T06:00:00'))
        self.assertEqual(buckets[-1].start, dt('2018-03-04T09:00:00'))

        self.check_all_intervals(buckets, 'buba', timedelta(seconds=3600), store)

    def test_get_bucket_with_2_size_hour_check_buckets(self):
        store = DummyStore(bucket_size=7200)
        inter = Interval(start=self.start, delta=timedelta(hours=27))
        buckets = list(store._interval_buckets('buba', interval=inter))

        self.assertEqual(buckets[0].start, dt('2018-03-03T06:00:00'))
        self.assertEqual(buckets[-1].start, dt('2018-03-04T08:00:00'))

        self.check_all_intervals(buckets, 'buba', timedelta(seconds=7200), store)

    def test_get_bucket_with_3_size_hour_check_buckets(self):
        store = DummyStore(bucket_size=3 * 3600)
        inter = Interval(start=self.start, delta=timedelta(hours=27))
        buckets = list(store._interval_buckets('buba', interval=inter))

        self.assertEqual(buckets[0].start, dt('2018-03-03T06:00:00'))
        self.assertEqual(buckets[-1].start, dt('2018-03-04T09:00:00'))

        self.check_all_intervals(buckets, 'buba', timedelta(seconds=3 * 3600), store)


    def check_all_intervals(self, buckets, name, delta, store):

        hour = buckets[0].start.hour
        for i, bucket in enumerate(buckets):
            # on beginning of each our we will get the 00 bucket
            if hour == bucket.start.hour:
                if i > 0:
                    self.assertEqual(bucket.start, buckets[i - 1].start + delta)
            else:
                self.assertEqual(0, bucket.start.minute)
                hour = bucket.start.hour

            self.assertEqual(bucket.name, name)
            self.assertEqual(bucket.length, delta.seconds)

            # make sure that random timestamp from inside that bucket lands
            # in that bucket
            timestamp = bucket.start + timedelta(seconds=randint(0, bucket.length - 1))
            same_bucket = store._get_bucket(name, timestamp)
            # we can get here a bucket from the next hour (every hour is) so it will be the same bucket
            # or first one from last hour
            if same_bucket.start.hour == bucket.start.hour:
                self.assertEqual(same_bucket.start, bucket.start)
            else:
                # start hour will be next hour it might be 0 at midnight hence modulo 24
                self.assertEqual(same_bucket.start.hour, (bucket.start.hour + 1) % 24)

                self.assertEqual(same_bucket.start.minute, 0)


class BaseStoreTestCase(TestCase):

    def test_assignment_of_bucket_size_raises(self):
        store = DummyStore()
        with self.assertRaises(AttributeError):
            store.bucket_size = 1

