import datetime
import unittest

from influxdb_client_3.write_client.client.write.point import EPOCH, Point


class TestPoint(unittest.TestCase):

    def test_epoch(self):
        self.assertEqual(EPOCH, datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc))

    def test_point(self):
        point = Point.measurement("h2o").tag("location", "europe").field("level", 2.2).time(1_000_000)
        self.assertEqual('h2o,location=europe level=2.2 1000000', point.to_line_protocol())

    def test_point_tag_order(self):
        point = Point.measurement("h2o") \
            .tag("drop", None) \
            .tag("rack", "r1") \
            .tag("host", "h1") \
            .tag("region", "us-east") \
            .field("level", 2)

        self.assertEqual('h2o,host=h1,rack=r1,region=us-east level=2i', point.to_line_protocol())
        self.assertEqual('h2o,region=us-east,host=h1,rack=r1 level=2i',
                         point.to_line_protocol(tag_order=["region", "", "host", "region", "missing"]))
