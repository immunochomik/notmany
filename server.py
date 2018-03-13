from datetime import timedelta

import tornado.web
import tornado.ioloop

from notmany.store.base import get_datetime, Interval

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


if __name__ == "__main__":
    port = 8888
    application = tornado.web.Application([
        (r"/metric/(.*?)", MetricHandler),
    ])
    print('Listen on {}'.format(port))
    application.listen(port=port)
    tornado.ioloop.IOLoop.current().start()
