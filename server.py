
import tornado.web
import tornado.ioloop

from notmany.store.base import dt, get_datetime, Interval
from notmany.core import retrieve, record

# TODO proper input validation
from notmany.store.file import Store

store = Store(directory='tests/store')

class MetricHandler(tornado.web.RequestHandler):
    def get(self, metric):

        start = get_datetime(self.get_query_argument(name='start'))
        delta = self.get_query_argument(name='delta', default=None)
        end = self.get_query_argument(name='end', default=None)
        if not delta and not end:
            raise tornado.web.MissingArgumentError('You have to define delta or end')

        interval = Interval(start=start, end=end, delta=delta)
        gen = Store.retrieve_raw(name=metric, interval=interval)
        self.write(gen)

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
