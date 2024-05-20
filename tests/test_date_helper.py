import unittest
from datetime import datetime, timezone

from dateutil import tz

from influxdb_client_3.write_client.client.util.date_utils import DateHelper, get_date_helper


class TestDateHelper(unittest.TestCase):

    def test_to_utc(self):
        date = get_date_helper().to_utc(datetime(2021, 4, 29, 20, 30, 10, 0))
        self.assertEqual(datetime(2021, 4, 29, 20, 30, 10, 0, timezone.utc), date)

    def test_to_utc_different_timezone(self):
        date = DateHelper(timezone=tz.gettz('ETC/GMT+2')).to_utc(datetime(2021, 4, 29, 20, 30, 10, 0))
        self.assertEqual(datetime(2021, 4, 29, 22, 30, 10, 0, timezone.utc), date)

    def test_parse(self):
        date = get_date_helper().parse_date("2021-03-20T15:59:10.607352Z")
        self.assertEqual(datetime(2021, 3, 20, 15, 59, 10, 607352, timezone.utc), date)
