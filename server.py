from collections import defaultdict
from datetime import timedelta, datetime

import tornado.web
import tornado.ioloop
from bkcharts import TimeSeries, output_file, show, save

from notmany.store.base import get_datetime, Interval, record_to_data

# TODO proper input validation, and enforce max delta
from notmany.store.file import Store, MAX_DELTA

store = Store(directory='tests/store', bucket_size=600)


class MetricHandler(tornado.web.RequestHandler):
    def get(self, metric):

        start = get_datetime(self.get_query_argument(name='start'))
        delta = self.get_query_argument(name='delta', default=None)
        end = self.get_query_argument(name='end', default=None)

        if delta is not None:
            delta = min(int(delta), MAX_DELTA)
            delta = timedelta(seconds=int(delta))
        elif not end:
            raise tornado.web.MissingArgumentError('You have to define delta or end')

        interval = Interval(start=start, end=end, delta=delta)

        for chunk in store.retrieve_raw(name=metric, interval=interval):
            self.write(chunk=chunk)
        self.finish()

    def post(self, metric):
        ts = get_datetime(self.get_body_argument("timestamp"))
        data = self.get_body_argument('data')
        store.record(
            name=metric,
            timestamp=ts,
            data=data
        )

class ChartHandler(tornado.web.RequestHandler):

    def get(self, name):
        start = get_datetime(self.get_query_argument(name='start'))
        delta = self.get_query_argument(name='delta', default=None)
        end = self.get_query_argument(name='end', default=None)

        if delta is not None:
            delta = min(int(delta), MAX_DELTA)
            delta = timedelta(seconds=int(delta))
        elif not end:
            raise tornado.web.MissingArgumentError('You have to define delta or end')

        interval = Interval(start=start, end=end, delta=delta)
        data = chart_data(records=store.retrieve(name=name, interval=interval))
        # output_file("stocks_timeseries.html")
        # p = TimeSeries(data, title=name, ylabel='Foo')
        # save(p)
        self.write('ok')


def chart_data(records):
    data = {
        'Date': list()
    }
    first = True

    for record in records:
        data['Date'].append(datetime.fromtimestamp(record[0]))

        if first:  # initialise data with first record from interval
            first = False
            for key, value in record_to_data(record[1]).items():
                data[key] = [value]
            continue

        for key, value in record_to_data(record[1]).items():
            if key in data:
                data[key].append(value)
    return data

if __name__ == "__main__":
    port = 8887
    application = tornado.web.Application([
        (r"/metric/(.*?)", MetricHandler),
        (r"/chart/(.*?)", ChartHandler),
    ])
    print('Listen on {}'.format(port))
    application.listen(port=port)
    tornado.ioloop.IOLoop.current().start()

