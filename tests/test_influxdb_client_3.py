import unittest
from unittest.mock import patch

from influxdb_client_3 import InfluxDBClient3
from tests.util import asyncio_run
from tests.util.mocks import ConstantFlightServer, ConstantData


class TestInfluxDBClient3(unittest.TestCase):

    @patch('influxdb_client_3._InfluxDBClient')
    @patch('influxdb_client_3._WriteApi')
    @patch('influxdb_client_3._QueryApi')
    def setUp(self, mock_query_api, mock_write_api, mock_influx_db_client):
        self.mock_influx_db_client = mock_influx_db_client
        self.mock_write_api = mock_write_api
        self.mock_query_api = mock_query_api
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
        self.assertEqual(self.client._query_api, self.mock_query_api.return_value)

    # test default token auth_scheme
    def test_token_auth_scheme_default(self):
        client = InfluxDBClient3(
            host="localhost",
            org="my_org",
            database="my_db",
            token="my_token",
        )
        self.assertEqual(client._client.auth_header_value, "Token my_token")

    # test explicit token auth_scheme
    def test_token_auth_scheme_explicit(self):
        client = InfluxDBClient3(
            host="localhost",
            org="my_org",
            database="my_db",
            token="my_token",
            auth_scheme="my_scheme"
        )
        self.assertEqual(client._client.auth_header_value, "my_scheme my_token")

    @asyncio_run
    async def test_query_async(self):
        with ConstantFlightServer() as server:
            client = InfluxDBClient3(
                host=f"http://localhost:{server.port}",
                org="my_org",
                database="my_db",
                token="my_token",
            )

            query = "SELECT * FROM my_data"

            table = await client.query_async(query=query, language="sql")

            result_list = table.to_pylist()

            cd = ConstantData()
            for item in cd.to_list():
                assert item in result_list

            assert {'data': 'database', 'reference': 'my_db', 'value': -1.0} in result_list
            assert {'data': 'sql_query', 'reference': query, 'value': -1.0} in result_list
            assert {'data': 'query_type', 'reference': 'sql', 'value': -1.0} in result_list


if __name__ == '__main__':
    unittest.main()
