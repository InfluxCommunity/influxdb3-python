import unittest
from unittest.mock import Mock, patch, ANY

from pyarrow.flight import Ticket

from influxdb_client_3 import InfluxDBClient3


class QueryTests(unittest.TestCase):

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
        self.client._flight_client = mock_flight_client
        self.client._write_api = mock_write_api

    def test_query_without_parameters(self):
        mock_do_get = Mock()
        self.client._flight_client.do_get = mock_do_get

        self.client.query('SELECT * FROM measurement')

        expected_ticket = Ticket(
            '{"database": "my_db", '
            '"sql_query": "SELECT * FROM measurement", '
            '"query_type": "sql"}'.encode('utf-8')
        )

        mock_do_get.assert_called_once_with(expected_ticket, ANY)

    def test_query_with_parameters(self):
        mock_do_get = Mock()
        self.client._flight_client.do_get = mock_do_get

        self.client.query('SELECT * FROM measurement WHERE time > $time', query_parameters={"time": "2021-01-01"})

        expected_ticket = Ticket(
            '{"database": "my_db", '
            '"sql_query": "SELECT * FROM measurement WHERE time > $time", '
            '"query_type": "sql", "params": {"time": "2021-01-01"}}'.encode('utf-8')
        )

        mock_do_get.assert_called_once_with(expected_ticket, ANY)
