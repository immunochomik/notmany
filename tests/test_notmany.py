from unittest import TestCase
from time import time

from notmany.store.base import Interval
from notmany.store.file import Store
from tests.utils import temporary_directory, dt, seconds


class SavingMetricsBaseTestCase(TestCase):

    def test_I_can_save_metrics(self):
        self.fail()

    def test_I_can_save_two_different_metrics(self):
        self.fail()

    def test_I_can_save_2_h_worth_of_metrics(self):
        self.fail()

    def test_I_can_save_48_h_worth_of_metrics(self):
        self.fail()


class SavingMetricsWeeksTestCase(TestCase):

    def setUp(self):
        self.start = dt('2018-03-03 12:30:00')

    def save_days_of_metric(self, store, days):
        start = time()
        count = 3600 * 24 * days
        for i in range(count):
            store.record('temp', timestamp=self.start + seconds(i), data='room:1')
        print('Saving records {} took {} bucket size {}'.format(count, time() - start, store.bucket_size))

    def test_I_can_save_and_retrieve_1_week_of_one_metric(self):
        with temporary_directory() as tem_dir:
            store = Store(directory=tem_dir, bucket_size=600)
            self.save_days_of_metric(store=store, days=7)

            start = time()
            records = list(store.retrieve(name='temp', interval=Interval(
                start=self.start,
                delta=seconds(3600 * 24 * 7)
            )))

            print('Retrieving {} records took {}'.format(len(records), time() - start))
            start = time()
            records.sort()
            print('Sorting took {}'.format(time() - start))
            self.assertEqual(len(records), 3600 * 24 * 7)

    def test_I_can_save_and_retrieve_2_week_of_one_metric(self):
        with temporary_directory() as tem_dir:
            store = Store(directory=tem_dir, bucket_size=600)
            self.save_days_of_metric(store=store, days=14)

            start = time()
            records = list(store.retrieve(name='temp', interval=Interval(
                start=self.start,
                delta=seconds(3600 * 24 * 14)
            )))

            print('Retrieving {} records took {}'.format(len(records), time() - start))
            start = time()
            records.sort()
            print('Sorting took {}'.format(time() - start))
            self.assertEqual(len(records), 3600 * 24 * 14)


    def test_I_can_save_4_weeks_of_2_metrics(self):
        self.fail()



class ReadingMetricsTestCase(TestCase):

    def test_I_can_read_last_10_minutes(self):
        self.fail()

    def test_I_can_read_last_2_hours(self):
        self.fail()

    def test_I_can_read_last_48_hours(self):
        self.fail()

    def test_I_can_read_last_week(self):
        self.fail()

    def test_I_can_read_last_month(self):
        self.fail()


