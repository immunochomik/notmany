
from unittest import TestCase


class ServerBase(TestCase):

    pass


class ServerTestCase(ServerBase):

    def test_I_can_send_metric(self):
        self.fail()


class ServerManyConnectionsTestCase(ServerBase):

    def test_I_can_send_metric_from_different_threads_simultaneously(self):
        self.fail()
