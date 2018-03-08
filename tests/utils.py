
from datetime import datetime

from notmany.store.base import FORMAT


def dt(date):
    return datetime.strptime(date, FORMAT)
