import contextlib
import tempfile
from datetime import datetime, timedelta

import os

import shutil
from uuid import uuid4

from notmany.store.base import FORMAT


def dt(date):
    return datetime.strptime(date, FORMAT)


@contextlib.contextmanager
def temporary_directory(sub_name=None):
    sub_name = sub_name or str(uuid4())
    tmp_dir = os.path.join(tempfile.gettempdir(), sub_name)
    if not os.path.exists(tmp_dir):
        os.mkdir(tmp_dir)
    try:
        yield tmp_dir
    finally:
        shutil.rmtree(tmp_dir)


def file_content(path, fname=None):
    if fname:
        path = os.path.join(path, fname)

    with open(path, 'r') as fp:
        return fp.read()


def seconds(num):
    return timedelta(seconds=num)

