import datetime
import unittest

from influxdb_client_3 import WritePrecision
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

    def test_point_field_types_and_time_conversion(self):
        point = Point.measurement("m") \
            .field("drop", None) \
            .field("flag", True) \
            .field("name", "abc") \
            .field("value", 1)

        self.assertEqual('m flag=true,name="abc",value=1i', point.to_line_protocol())

        dt = datetime.datetime(1970, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc)
        self.assertEqual('m value=1i 1000000000',
                         Point.measurement("m").field("value", 1).time(dt, WritePrecision.NS).to_line_protocol())
        self.assertEqual('m value=1i 1000000',
                         Point.measurement("m").field("value", 1).time(dt, WritePrecision.US).to_line_protocol())
        self.assertEqual('m value=1i 1000',
                         Point.measurement("m").field("value", 1).time(dt, WritePrecision.MS).to_line_protocol())
        self.assertEqual('m value=1i 1',
                         Point.measurement("m").field("value", 1).time(dt, WritePrecision.S).to_line_protocol())
        self.assertEqual('m value=1i 1000000',
                         Point.measurement("m").field("value", 1)
                         .time(datetime.timedelta(seconds=1), WritePrecision.US).to_line_protocol())
        self.assertEqual('m value=1i 1',
                         Point.measurement("m").field("value", 1)
                         .time("1970-01-01T00:00:01Z", WritePrecision.S).to_line_protocol())

        with self.assertRaisesRegex(ValueError, 'not supported'):
            Point.measurement("m").field("bad", object()).to_line_protocol()
        with self.assertRaises(ValueError):
            Point.measurement("m").field("value", 1).time([]).to_line_protocol()
