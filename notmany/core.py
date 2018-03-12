
from .store.file import Store


store = Store()


def record(name, timestamp, data):
    store.record(name=name, timestamp=timestamp, data=data)


def retrieve(name, interval=None):
    return store.retrieve(name=name, interval=interval)
