import datetime
import unittest

from influxdb_client_3.write_client.client.write.point import EPOCH


class TestPoint(unittest.TestCase):

    def test_epoch(self):
        self.assertEqual(EPOCH, datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc))
