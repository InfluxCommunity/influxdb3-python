import unittest
from unittest.mock import patch, Mock
from influxdb_client_3 import InfluxDBClient3

class TestInfluxDBClient3(unittest.TestCase):

    @patch('influxdb_client_3._InfluxDBClient')
    @patch('influxdb_client_3._WriteApi')
    @patch('influxdb_client_3.FlightClient')
    def setUp(self, mock_flight_client, mock_write_api, mock_influx_db_client):
        self.mock_influx_db_client = mock_influx_db_client
        self.mock_write_api = mock_write_api
        self.mock_flight_client = mock_flight_client
        self.client = InfluxDBClient3(
            host="localhost",
            org="my_org",
            database="my_db",
            token="my_token"
        )

    def test_init(self):
        self.assertEqual(self.client._org, "my_org")
        self.assertEqual(self.client._database, "my_db")
        self.assertEqual(self.client._client, self.mock_influx_db_client.return_value)
        self.assertEqual(self.client._write_api, self.mock_write_api.return_value)
        self.assertEqual(self.client._flight_client, self.mock_flight_client.return_value)

    @patch('influxdb_client_3._WriteApi.write')
    def test_write(self, mock_write):
        record = "test_record"
        self.client.write(record=record)
        mock_write.assert_called_once_with(bucket=self.client._database, record=record)

    # Add more tests for other methods


if __name__ == '__main__':
    unittest.main()