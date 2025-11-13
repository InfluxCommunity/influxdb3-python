import re
import unittest
from unittest.mock import patch

from pytest_httpserver import HTTPServer

from influxdb_client_3 import InfluxDBClient3, WritePrecision, DefaultWriteOptions, Point, WriteOptions, WriteType, \
    write_client_options
from influxdb_client_3.exceptions import InfluxDB3ClientQueryError
from influxdb_client_3.write_client.rest import ApiException
from tests.util import asyncio_run
from tests.util.mocks import ConstantFlightServer, ConstantData, ErrorFlightServer


def http_server():
    httpserver = HTTPServer()
    httpserver.start()
    return httpserver


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
        self.http_server = http_server()

    def tearDown(self):
        if self.http_server is not None:
            self.http_server.stop()

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

    def test_write_options(self):
        client = InfluxDBClient3(
            host="localhost",
            org="my_org",
            token="my_token",
            auth_scheme="my_scheme",
            write_client_options=write_client_options(
                success_callback=lambda _: True,
                error_callback=lambda _: False,
                extra_arg="ignored",
                write_options=WriteOptions(write_type=WriteType.synchronous,
                                           max_retries=0,
                                           max_retry_time=0,
                                           max_retry_delay=0,
                                           timeout=30_000,
                                           flush_interval=500,))
        )

        self.assertIsInstance(client._write_client_options["write_options"], WriteOptions)
        self.assertTrue(client._write_client_options["success_callback"]("an_arg"))
        self.assertFalse(client._write_client_options["error_callback"]("an_arg"))
        self.assertEqual("ignored", client._write_client_options["extra_arg"])
        self.assertEqual(30_000, client._write_client_options["write_options"].timeout)
        self.assertEqual(0, client._write_client_options["write_options"].max_retries)
        self.assertEqual(0, client._write_client_options["write_options"].max_retry_time)
        self.assertEqual(0, client._write_client_options["write_options"].max_retry_delay)
        self.assertEqual(WriteType.synchronous, client._write_client_options["write_options"].write_type)
        self.assertEqual(500, client._write_client_options["write_options"].flush_interval)

    def test_default_write_options(self):
        client = InfluxDBClient3(
            host="localhost",
            token="my_token",
            org="my_org",
            database="my_db",
        )

        self.assertEqual(DefaultWriteOptions.write_type.value,
                         client._write_client_options["write_options"].write_type)
        self.assertEqual(DefaultWriteOptions.no_sync.value, client._write_client_options["write_options"].no_sync)
        self.assertEqual(DefaultWriteOptions.write_precision.value,
                         client._write_client_options["write_options"].write_precision)
        self.assertEqual(DefaultWriteOptions.timeout.value, client._write_client_options["write_options"].timeout)

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

    def test_write_api_custom_options_no_error(self):
        write_options = WriteOptions(write_type=WriteType.batching)
        write_client_option = {'write_options': write_options}
        client = InfluxDBClient3(write_client_options=write_client_option)
        try:
            client._write_api._write_batching("bucket", "org", Point.measurement("test"), None)
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"Write API with default options raised an exception: {str(e)}")
        finally:
            client._write_api._on_complete()  # abort batch writes - otherwise test cycles through urllib3 retries

    def test_default_client(self):
        expected_precision = DefaultWriteOptions.write_precision.value
        expected_write_type = DefaultWriteOptions.write_type.value
        expected_no_sync = DefaultWriteOptions.no_sync.value

        import os
        try:
            os.environ["INFLUX_HOST"]
        except KeyError:
            os.environ["INFLUX_HOST"] = "http://my-influx.io"

        try:
            os.environ["INFLUX_TOKEN"]
        except KeyError:
            os.environ["INFLUX_TOKEN"] = "my-token"

        try:
            os.environ["INFLUX_DATABASE"]
        except KeyError:
            os.environ["INFLUX_DATABASE"] = "my-bucket"

        def verify_client_write_options(c):
            write_options = c._write_client_options.get('write_options')
            self.assertEqual(write_options.write_precision, expected_precision)
            self.assertEqual(write_options.write_type, expected_write_type)
            self.assertEqual(write_options.no_sync, expected_no_sync)

            self.assertEqual(c._write_api._write_options.write_precision, expected_precision)
            self.assertEqual(c._write_api._write_options.write_type, expected_write_type)
            self.assertEqual(c._write_api._write_options.no_sync, expected_no_sync)

        env_client = InfluxDBClient3.from_env()
        verify_client_write_options(env_client)

        default_client = InfluxDBClient3()
        verify_client_write_options(default_client)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_ORG': 'test_org',
                               'INFLUX_PRECISION': WritePrecision.MS, 'INFLUX_AUTH_SCHEME': 'custom_scheme',
                               'INFLUX_GZIP_THRESHOLD': '2000', 'INFLUX_WRITE_NO_SYNC': 'true',
                               'INFLUX_WRITE_TIMEOUT': '1234', 'INFLUX_QUERY_TIMEOUT': '5678'})
    def test_from_env_all_env_vars_set(self):
        client = InfluxDBClient3.from_env()
        self.assertIsInstance(client, InfluxDBClient3)
        self.assertEqual(client._token, "test_token")
        self.assertEqual(client._client.url, "https://localhost:443")
        self.assertEqual(client._client.auth_header_value, f"custom_scheme {client._token}")
        self.assertEqual(client._database, "test_db")
        self.assertEqual(client._org, "test_org")
        self.assertEqual(client._client.api_client.rest_client.configuration.gzip_threshold, 2000)

        write_options = client._write_client_options.get("write_options")
        self.assertEqual(write_options.write_precision, WritePrecision.MS)
        self.assertEqual(write_options.no_sync, True)
        self.assertEqual(1234, write_options.timeout)
        self.assertEqual(5.678, client._query_api._default_timeout)

        client._write_api._point_settings = {}

    @patch.dict('os.environ', {'INFLUX_HOST': "", 'INFLUX_TOKEN': "",
                               'INFLUX_DATABASE': "", 'INFLUX_ORG': ""})
    def test_from_env_missing_variables(self):
        with self.assertRaises(ValueError) as context:
            InfluxDBClient3.from_env()
        self.assertIn("Missing required environment variables", str(context.exception))

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_PRECISION': WritePrecision.S})
    def test_parse_valid_write_precision(self):
        client = InfluxDBClient3.from_env()
        self.assertIsInstance(client, InfluxDBClient3)
        self.assertEqual(client._write_client_options.get('write_options').write_precision, WritePrecision.S)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_PRECISION': "microsecond"})
    def test_parse_valid_long_write_precision_us(self):
        client = InfluxDBClient3.from_env()
        self.assertIsInstance(client, InfluxDBClient3)
        self.assertEqual(client._write_client_options.get('write_options').write_precision, WritePrecision.US)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_PRECISION': "nanosecond"})
    def test_parse_valid_long_write_precision_ns(self):
        client = InfluxDBClient3.from_env()
        self.assertIsInstance(client, InfluxDBClient3)
        self.assertEqual(client._write_client_options.get('write_options').write_precision, WritePrecision.NS)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_PRECISION': 'invalid_value'})
    def test_parse_invalid_write_precision(self):
        with self.assertRaises(ValueError) as context:
            InfluxDBClient3.from_env()
        self.assertIn("Invalid precision value: invalid_value", str(context.exception))

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_WRITE_NO_SYNC': 'true'})
    def test_parse_write_no_sync_true(self):
        client = InfluxDBClient3.from_env()
        self.assertIsInstance(client, InfluxDBClient3)
        write_options = client._write_client_options.get("write_options")
        self.assertEqual(write_options.no_sync, True)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_WRITE_NO_SYNC': 'TrUe'})
    def test_parse_write_no_sync_true_mixed_chars(self):
        client = InfluxDBClient3.from_env()
        self.assertIsInstance(client, InfluxDBClient3)
        write_options = client._write_client_options.get("write_options")
        self.assertEqual(write_options.no_sync, True)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_WRITE_NO_SYNC': 'false'})
    def test_parse_write_no_sync_false(self):
        client = InfluxDBClient3.from_env()
        self.assertIsInstance(client, InfluxDBClient3)
        write_options = client._write_client_options.get("write_options")
        self.assertEqual(write_options.no_sync, False)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_WRITE_NO_SYNC': 'anything-else'})
    def test_parse_write_no_sync_anything_else_is_false(self):
        client = InfluxDBClient3.from_env()
        self.assertIsInstance(client, InfluxDBClient3)
        write_options = client._write_client_options.get("write_options")
        self.assertEqual(write_options.no_sync, False)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_WRITE_TIMEOUT': '6789'})
    def test_parse_valid_write_timeout(self):
        client = InfluxDBClient3.from_env()
        self.assertIsInstance(client, InfluxDBClient3)
        write_options = client._write_client_options.get("write_options")
        self.assertEqual(6789, write_options.timeout)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_WRITE_TIMEOUT': 'foo'})
    def test_parse_invalid_write_timeout_domain(self):
        with self.assertRaisesRegex(ValueError, ".*Must be a number.*"):
            InfluxDBClient3.from_env()

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_WRITE_TIMEOUT': '-42'})
    def test_parse_invalid_write_timeout_range(self):
        with self.assertRaisesRegex(ValueError, ".*Must be non-negative.*"):
            InfluxDBClient3.from_env()

    def test_query_with_arrow_error(self):
        f = ErrorFlightServer()
        with InfluxDBClient3(f"http://localhost:{f.port}", "my_org", "my_db", "my_token") as c:
            with self.assertRaises(InfluxDB3ClientQueryError) as err:
                c.query("SELECT * FROM my_data")
            self.assertIn("Error while executing query", str(err.exception))

    @asyncio_run
    async def test_async_query_with_arrow_error(self):
        f = ErrorFlightServer()
        with InfluxDBClient3(f"http://localhost:{f.port}", "my_org", "my_db", "my_token") as c:
            with self.assertRaises(InfluxDB3ClientQueryError) as err:
                await c.query_async("SELECT * FROM my_data")
            self.assertIn("Error while executing query", str(err.exception))

    def test_get_version_header_success(self):
        server = self.http_server
        server.expect_request(re.compile(".*")).respond_with_json(
            headers={"X-Influxdb-Version": "1.8.2"},
            response_json={"version": "3.0"}
        )
        version = InfluxDBClient3(
            host=f'http://{server.host}:{server.port}', org="ORG", database="DB", token="TOKEN"
        ).get_server_version()
        assert version == "1.8.2"

    def test_get_version_in_body_success(self):
        server = self.http_server
        server.expect_request('/ping').respond_with_json(
            response_json={"version": "3.0"},
        )
        version = InfluxDBClient3(
            host=f'http://{server.host}:{server.port}', org="ORG", database="DB", token="TOKEN"
        ).get_server_version()
        assert version == "3.0"

    def test_get_version_empty(self):
        server = self.http_server
        server.expect_request("/ping").respond_with_data(
            headers={"abc": "1.8.2"},
        )

        version = InfluxDBClient3(
            host=f'http://{server.host}:{server.port}', org="ORG", database="DB", token="TOKEN"
        ).get_server_version()
        assert version is None

    def test_get_version_fail(self):
        server = self.http_server
        server.expect_request("/ping").respond_with_json(
            response_json={"error": "error"},
            status=400
        )
        with self.assertRaises(ApiException):
            InfluxDBClient3(
                host=f'http://{server.host}:{server.port}', org="ORG", database="DB", token="TOKEN"
            ).get_server_version()

    def test_url_with_path_prefix(self):
        server = self.http_server
        server.expect_request('/prefix/ping').respond_with_json(
            response_json={"version": "3.0"},
        )
        version = InfluxDBClient3(
            host=f'http://{server.host}:{server.port}/prefix',
            org="ORG",
            database="DB",
            token="TOKEN"
        ).get_server_version()
        assert version == "3.0"

    def test_url_lack_path_prefix(self):
        server = self.http_server
        server.expect_request('/prefix/ping').respond_with_json(
            response_json={"version": "3.0"},
        )
        try:
            InfluxDBClient3(
                host=f'http://{server.host}:{server.port}',
                org="ORG",
                database="DB",
                token="TOKEN"
            ).get_server_version()
        except ApiException:
            self.assertRaises(ApiException)


if __name__ == '__main__':
    unittest.main()
