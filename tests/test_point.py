import datetime
import unittest

from influxdb_client_3.write_client.client.write.point import EPOCH, Point


class TestPoint(unittest.TestCase):

    def test_epoch(self):
        self.assertEqual(EPOCH, datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc))

    def test_point(self):
        point = Point.measurement("h2o").tag("location", "europe").field("level", 2.2).time(1_000_000)
        self.assertEqual('h2o,location=europe level=2.2 1000000', point.to_line_protocol())
