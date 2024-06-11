import unittest
import struct
from unittest.mock import Mock, patch, ANY

from pyarrow import (
    array,
    Table
)

from pyarrow.flight import (
    FlightServerBase,
    FlightUnauthenticatedError,
    GeneratorStream,
    ServerMiddleware,
    ServerMiddlewareFactory,
    ServerAuthHandler,
    Ticket
)

from influxdb_client_3 import InfluxDBClient3
from influxdb_client_3.version import USER_AGENT


def case_insensitive_header_lookup(headers, lkey):
    """Lookup the value of a given key in the given headers.
       The lkey is case-insensitive.
    """
    for key in headers:
        if key.lower() == lkey.lower():
            return headers.get(key)


class NoopAuthHandler(ServerAuthHandler):
    """A no-op auth handler - as seen in pyarrow tests"""

    def authenticate(self, outgoing, incoming):
        """Do nothing"""

    def is_valid(self, token):
        """
        Return an empty string
        N.B. Returning None causes Type error
        :param token:
        :return:
        """
        return ""


_req_headers = {}


class HeaderCheckServerMiddlewareFactory(ServerMiddlewareFactory):
    """Factory to create HeaderCheckServerMiddleware and check header values"""
    def start_call(self, info, headers):
        auth_header = case_insensitive_header_lookup(headers, "Authorization")
        values = auth_header[0].split(' ')
        if values[0] != 'Bearer':
            raise FlightUnauthenticatedError("Token required")
        global _req_headers
        _req_headers = headers
        return HeaderCheckServerMiddleware(values[1])


class HeaderCheckServerMiddleware(ServerMiddleware):
    """
    Middleware needed to catch request headers via factory
    N.B. As found in pyarrow tests
    """
    def __init__(self, token):
        self.token = token

    def sending_headers(self):
        return {'authorization': 'Bearer ' + self.token}


class HeaderCheckFlightServer(FlightServerBase):
    """Mock server handle gRPC do_get calls"""
    def do_get(self, context, ticket):
        """Return something to avoid needless errors"""
        data = [
            array([b"Vltava", struct.pack('<i', 105), b"FM"])
        ]
        table = Table.from_arrays(data, names=['a'])
        return GeneratorStream(
            table.schema,
            self.number_batches(table),
            options={})

    @staticmethod
    def number_batches(table):
        for idx, batch in enumerate(table.to_batches()):
            buf = struct.pack('<i', idx)
            yield batch, buf


def test_influx_default_query_headers():
    with HeaderCheckFlightServer(
            auth_handler=NoopAuthHandler(),
            middleware={"check": HeaderCheckServerMiddlewareFactory()}) as server:
        global _req_headers
        _req_headers = {}
        client = InfluxDBClient3(
            host=f'http://localhost:{server.port}',
            org='test_org',
            databse='test_db',
            token='TEST_TOKEN'
        )
        client.query('SELECT * FROM test')
        assert len(_req_headers) > 0
        assert _req_headers['authorization'][0] == "Bearer TEST_TOKEN"
        assert _req_headers['user-agent'][0].find(USER_AGENT) > -1
        _req_headers = {}


class TestQuery(unittest.TestCase):

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
