import re
import unittest
from collections import defaultdict
from unittest.mock import patch
from pytest_httpserver import HTTPServer

from influxdb_client_3 import InfluxDBClient3, WritePrecision, DefaultWriteOptions, Point, WriteOptions, WriteType, \
    write_client_options
from influxdb_client_3.exceptions import InfluxDB3ClientQueryError
from influxdb_client_3.write_client.rest import ApiException
from tests.util import asyncio_run
from tests.util.mocks import ConstantFlightServer, ConstantData, ErrorFlightServer

import pandas as pd

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False


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
                                           flush_interval=500,
                                           tag_order=["region", "", "host", "region"]))
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
        self.assertEqual(["region", "host"], client._write_client_options["write_options"].tag_order)

        with self.assertRaisesRegex(TypeError, "tag_order must be an iterable of strings, not str/bytes"):
            WriteOptions(tag_order="region,host")

        with self.assertRaisesRegex(TypeError, "tag_order entries must be strings"):
            WriteOptions(tag_order=["region", 1])

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
        self.assertEqual(DefaultWriteOptions.accept_partial.value,
                         client._write_client_options["write_options"].accept_partial)
        self.assertEqual(DefaultWriteOptions.use_v2_api.value,
                         client._write_client_options["write_options"].use_v2_api)
        self.assertEqual(DefaultWriteOptions.write_precision.value,
                         client._write_client_options["write_options"].write_precision)
        self.assertEqual(DefaultWriteOptions.timeout.value, client._write_client_options["write_options"].timeout)
        self.assertEqual([], client._write_client_options["write_options"].tag_order)

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

            assert {'data': 'database', 'reference': 'my_db', 'value': -1.0, 'null_field': None} in result_list
            assert {'data': 'sql_query', 'reference': query, 'value': -1.0, 'null_field': None} in result_list
            assert {'data': 'query_type', 'reference': 'sql', 'value': -1.0, 'null_field': None} in result_list

    def test_write_api_custom_options_no_error(self):
        write_options = WriteOptions(write_type=WriteType.batching)
        write_client_option = {'write_options': write_options}
        client = InfluxDBClient3(write_client_options=write_client_option)
        sync_client = None
        try:
            client._write_api._write_batching("bucket", "org", Point.measurement("test"), None)
            client._write_api._write_batching("bucket", "org", {
                "measurement": "test",
                "fields": {"value": 1}
            }, None)
            df = pd.DataFrame({
                "value": [1, 2],
            }, index=pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-01T01:00:00Z"]))
            client._write_api._write_batching(
                "bucket", "org", df, None,
                data_frame_measurement_name="test_measurement",
            )
            point = Point.measurement("test").tag("host", "h1").field("value", 1).time(1, WritePrecision.S)
            payload = defaultdict(list)
            client._write_api._serialize(point, WritePrecision.NS, payload, tag_order=["host"])
            self.assertIn(WritePrecision.S, payload)

            payload_forced = defaultdict(list)
            client._write_api._serialize(point, WritePrecision.NS, payload_forced,
                                         precision_from_point=False, tag_order=["host"])
            self.assertIn(WritePrecision.NS, payload_forced)

            sync_client = InfluxDBClient3(
                host="localhost",
                org="my_org",
                database="my_db",
                token="my_token",
                write_client_options=write_client_options(
                    write_options=WriteOptions(write_type=WriteType.synchronous))
            )
            with patch.object(sync_client._write_api, "_post_write", return_value=None) as mock_post:
                sync_point = Point.measurement("measurement") \
                    .tag("host", "h1") \
                    .tag("region", "us-east") \
                    .field("value", 1)
                sync_client.write(record=sync_point, tag_order=["region", "", "host", "region"])

                args, kwargs = mock_post.call_args
                body = kwargs.get("body")
                if body is None and len(args) >= 4:
                    body = args[3]
                if isinstance(body, bytes):
                    body = body.decode("utf-8")
                self.assertIn("measurement,region=us-east,host=h1 value=1i", body)

            self.assertTrue(True)
        except Exception as e:
            self.fail(f"Write API with default options raised an exception: {str(e)}")
        finally:
            client._write_api._on_complete()  # abort batch writes - otherwise test cycles through urllib3 retries
            if sync_client is not None:
                sync_client.close()

    def test_default_client(self):
        expected_precision = DefaultWriteOptions.write_precision.value
        expected_write_type = DefaultWriteOptions.write_type.value
        expected_no_sync = DefaultWriteOptions.no_sync.value
        expected_accept_partial = DefaultWriteOptions.accept_partial.value
        expected_use_v2_api = DefaultWriteOptions.use_v2_api.value

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
            self.assertEqual(write_options.accept_partial, expected_accept_partial)
            self.assertEqual(write_options.use_v2_api, expected_use_v2_api)
            self.assertEqual(write_options.tag_order, [])

            self.assertEqual(c._write_api._write_options.write_precision, expected_precision)
            self.assertEqual(c._write_api._write_options.write_type, expected_write_type)
            self.assertEqual(c._write_api._write_options.no_sync, expected_no_sync)
            self.assertEqual(c._write_api._write_options.accept_partial, expected_accept_partial)
            self.assertEqual(c._write_api._write_options.use_v2_api, expected_use_v2_api)
            self.assertEqual(c._write_api._write_options.tag_order, [])

        env_client = InfluxDBClient3.from_env()
        verify_client_write_options(env_client)

        default_client = InfluxDBClient3()
        verify_client_write_options(default_client)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_ORG': 'test_org',
                               'INFLUX_PRECISION': WritePrecision.MS, 'INFLUX_AUTH_SCHEME': 'custom_scheme',
                               'INFLUX_GZIP_THRESHOLD': '2000', 'INFLUX_WRITE_NO_SYNC': 'true',
                               'INFLUX_WRITE_ACCEPT_PARTIAL': 'false', 'INFLUX_WRITE_USE_V2_API': 'true',
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
        self.assertEqual(write_options.accept_partial, False)
        self.assertEqual(write_options.use_v2_api, True)
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
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_WRITE_ACCEPT_PARTIAL': 'false'})
    def test_parse_write_accept_partial_false(self):
        client = InfluxDBClient3.from_env()
        self.assertIsInstance(client, InfluxDBClient3)
        write_options = client._write_client_options.get("write_options")
        self.assertEqual(write_options.accept_partial, False)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_WRITE_USE_V2_API': 'true'})
    def test_parse_write_use_v2_api_true(self):
        client = InfluxDBClient3.from_env()
        self.assertIsInstance(client, InfluxDBClient3)
        write_options = client._write_client_options.get("write_options")
        self.assertEqual(write_options.use_v2_api, True)

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

    def assertGrpcCompressionDisabled(self, client, disabled):
        """Assert whether gRPC compression is disabled for the client."""
        self.assertIsInstance(client, InfluxDBClient3)
        generic_options = dict(client._query_api._flight_client_options['generic_options'])
        if disabled:
            self.assertEqual(generic_options.get('grpc.compression_enabled_algorithms_bitset'), 1)
        else:
            self.assertIsNone(generic_options.get('grpc.compression_enabled_algorithms_bitset'))

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_DISABLE_GRPC_COMPRESSION': 'true'})
    def test_from_env_disable_grpc_compression_true(self):
        client = InfluxDBClient3.from_env()
        self.assertGrpcCompressionDisabled(client, True)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_DISABLE_GRPC_COMPRESSION': 'TrUe'})
    def test_from_env_disable_grpc_compression_true_mixed_case(self):
        client = InfluxDBClient3.from_env()
        self.assertGrpcCompressionDisabled(client, True)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_DISABLE_GRPC_COMPRESSION': '1'})
    def test_from_env_disable_grpc_compression_one(self):
        client = InfluxDBClient3.from_env()
        self.assertGrpcCompressionDisabled(client, True)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_DISABLE_GRPC_COMPRESSION': 'false'})
    def test_from_env_disable_grpc_compression_false(self):
        client = InfluxDBClient3.from_env()
        self.assertGrpcCompressionDisabled(client, False)

    @patch.dict('os.environ', {'INFLUX_HOST': 'localhost', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_DISABLE_GRPC_COMPRESSION': 'anything-else'})
    def test_from_env_disable_grpc_compression_anything_else_is_false(self):
        client = InfluxDBClient3.from_env()
        self.assertGrpcCompressionDisabled(client, False)

    def test_disable_grpc_compression_parameter_true(self):
        client = InfluxDBClient3(
            host="localhost",
            org="my_org",
            database="my_db",
            token="my_token",
            disable_grpc_compression=True
        )
        self.assertGrpcCompressionDisabled(client, True)

    def test_disable_grpc_compression_parameter_false(self):
        client = InfluxDBClient3(
            host="localhost",
            org="my_org",
            database="my_db",
            token="my_token",
            disable_grpc_compression=False
        )
        self.assertGrpcCompressionDisabled(client, False)

    def test_disable_grpc_compression_default_is_false(self):
        client = InfluxDBClient3(
            host="localhost",
            org="my_org",
            database="my_db",
            token="my_token",
        )
        self.assertGrpcCompressionDisabled(client, False)

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


class TestWriteDataFrame(unittest.TestCase):
    """Tests for the write_dataframe() method."""

    @patch('influxdb_client_3._InfluxDBClient')
    @patch('influxdb_client_3._WriteApi')
    @patch('influxdb_client_3._QueryApi')
    def setUp(self, mock_query_api, mock_write_api, mock_influx_db_client):
        self.mock_write_api = mock_write_api
        self.client = InfluxDBClient3(
            host="localhost",
            org="my_org",
            database="my_db",
            token="my_token"
        )

    def test_write_dataframe_with_pandas(self):
        """Test write_dataframe() with a pandas DataFrame."""
        df = pd.DataFrame({
            'time': pd.to_datetime(['2024-01-01', '2024-01-02']),
            'city': ['London', 'Paris'],
            'temperature': [15.0, 18.0]
        })

        self.client.write_dataframe(
            df,
            measurement='weather',
            timestamp_column='time',
            tags=['city']
        )

        # Verify _write_api.write was called with correct parameters
        self.client._write_api.write.assert_called_once()
        call_kwargs = self.client._write_api.write.call_args[1]
        self.assertEqual(call_kwargs['bucket'], 'my_db')
        self.assertEqual(call_kwargs['data_frame_measurement_name'], 'weather')
        self.assertEqual(call_kwargs['data_frame_tag_columns'], ['city'])
        self.assertEqual(call_kwargs['data_frame_timestamp_column'], 'time')

    def test_write_dataframe_with_custom_database(self):
        """Test write_dataframe() with a custom database."""
        df = pd.DataFrame({
            'time': pd.to_datetime(['2024-01-01']),
            'value': [42.0]
        })

        self.client.write_dataframe(
            df,
            measurement='test',
            timestamp_column='time',
            database='other_db'
        )

        call_kwargs = self.client._write_api.write.call_args[1]
        self.assertEqual(call_kwargs['bucket'], 'other_db')

    def test_write_dataframe_with_timezone(self):
        """Test write_dataframe() with timestamp timezone."""
        df = pd.DataFrame({
            'time': pd.to_datetime(['2024-01-01']),
            'value': [42.0]
        })

        self.client.write_dataframe(
            df,
            measurement='test',
            timestamp_column='time',
            timestamp_timezone='UTC'
        )

        call_kwargs = self.client._write_api.write.call_args[1]
        self.assertEqual(call_kwargs['data_frame_timestamp_timezone'], 'UTC')

    def test_write_dataframe_raises_type_error_for_invalid_input(self):
        """Test write_dataframe() raises TypeError for non-DataFrame input."""
        with self.assertRaises(TypeError) as context:
            self.client.write_dataframe(
                [1, 2, 3],  # A list, not a DataFrame
                measurement='test',
                timestamp_column='time'
            )
        self.assertIn("Expected a pandas or polars DataFrame", str(context.exception))
        self.assertIn("list", str(context.exception))

    def test_write_dataframe_raises_type_error_for_dict(self):
        """Test write_dataframe() raises TypeError for dict input."""
        with self.assertRaises(TypeError) as context:
            self.client.write_dataframe(
                {'time': [1, 2], 'value': [10, 20]},
                measurement='test',
                timestamp_column='time'
            )
        self.assertIn("Expected a pandas or polars DataFrame", str(context.exception))

    @unittest.skipUnless(HAS_POLARS, "Polars not installed")
    def test_write_dataframe_with_polars(self):
        """Test write_dataframe() with a polars DataFrame."""
        df = pl.DataFrame({
            'time': ['2024-01-01', '2024-01-02'],
            'city': ['London', 'Paris'],
            'temperature': [15.0, 18.0]
        })

        self.client.write_dataframe(
            df,
            measurement='weather',
            timestamp_column='time',
            tags=['city']
        )

        # Verify _write_api.write was called with correct parameters
        self.client._write_api.write.assert_called_once()
        call_kwargs = self.client._write_api.write.call_args[1]
        self.assertEqual(call_kwargs['data_frame_measurement_name'], 'weather')
        self.assertEqual(call_kwargs['data_frame_tag_columns'], ['city'])


class TestQueryDataFrame(unittest.TestCase):
    """Tests for the query_dataframe() method."""

    def test_query_dataframe_returns_pandas_by_default(self):
        """Test query_dataframe() returns pandas DataFrame by default."""
        with ConstantFlightServer() as server:
            client = InfluxDBClient3(
                host=f"http://localhost:{server.port}",
                org="my_org",
                database="my_db",
                token="my_token",
            )

            result = client.query_dataframe("SELECT * FROM test")

            # Should return a pandas DataFrame
            self.assertIsInstance(result, pd.DataFrame)

    def test_query_dataframe_with_sql_language(self):
        """Test query_dataframe() with explicit SQL language."""
        with ConstantFlightServer() as server:
            client = InfluxDBClient3(
                host=f"http://localhost:{server.port}",
                org="my_org",
                database="my_db",
                token="my_token",
            )

            result = client.query_dataframe("SELECT * FROM test", language="sql")
            self.assertIsInstance(result, pd.DataFrame)

    @unittest.skipUnless(HAS_POLARS, "Polars not installed")
    def test_query_dataframe_returns_polars_when_requested(self):
        """Test query_dataframe() returns polars DataFrame when frame_type='polars'."""
        with ConstantFlightServer() as server:
            client = InfluxDBClient3(
                host=f"http://localhost:{server.port}",
                org="my_org",
                database="my_db",
                token="my_token",
            )

            result = client.query_dataframe("SELECT * FROM test", frame_type="polars")

            # Should return a polars DataFrame
            self.assertIsInstance(result, pl.DataFrame)

    @patch('influxdb_client_3.polars', False)
    def test_query_dataframe_raises_import_error_for_polars_when_not_installed(self):
        """Test query_dataframe() raises ImportError when polars is requested but not installed."""
        with ConstantFlightServer() as server:
            client = InfluxDBClient3(
                host=f"http://localhost:{server.port}",
                org="my_org",
                database="my_db",
                token="my_token",
            )

            with self.assertRaises(ImportError) as context:
                client.query_dataframe("SELECT * FROM test", frame_type="polars")
            self.assertIn("Polars is not installed", str(context.exception))
            self.assertIn("pip install polars", str(context.exception))


if __name__ == '__main__':
    unittest.main()
