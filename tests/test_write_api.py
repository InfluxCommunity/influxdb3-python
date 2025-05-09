# tests/test_write_api.py

import unittest
from unittest import mock

from influxdb_client_3 import Point
from influxdb_client_3.write_client.client.write_api import WriteApi
from influxdb_client_3.write_client.domain import WritePrecision


class TestWriteApi(unittest.TestCase):

    def setUp(self):
        self.mock_client = mock.MagicMock()
        self.mock_write_options = mock.Mock()
        self.mock_point_settings = mock.Mock()
        self.write_api = WriteApi(self.mock_client, self.mock_write_options, self.mock_point_settings)

    @mock.patch("influxdb_client_3.write_client.client.write_api.WriteApi._write_batching")
    def test_write_batching_with_bytes(self, mock_write_batching):
        bucket = "my_bucket"
        org = "my_org"
        # data = b"test_data"
        precision = WritePrecision.NS
        point = Point.measurement("h2o").field("val", 1).time(1257894000123456000, write_precision=precision)

        self.write_api._write_batching(bucket, org, point, precision)
        mock_write_batching.assert_called_once_with(bucket, org, point, precision)
