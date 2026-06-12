import asyncio
import json
import unittest
import uuid
from unittest import mock

import pytest
from urllib3 import response
from urllib3.exceptions import ConnectTimeoutError

from influxdb_client_3 import InfluxDBClient3, InfluxDBError
from influxdb_client_3.exceptions import InfluxDBPartialWriteError
from influxdb_client_3.version import VERSION
from influxdb_client_3.write_client.rest import ApiException

_package = "influxdb3-python"
_sentHeaders = {}


class WriteApiTests(unittest.TestCase):
    received_timeout_total = None

    def mock_urllib3_timeout_request(method,
                                     url,
                                     **urlopen_kw):
        if urlopen_kw.get('timeout', None) is not None:
            WriteApiTests.received_timeout_total = urlopen_kw['timeout'].total
            raise ConnectTimeoutError()

        return response.HTTPResponse(status=200, version=4, reason="OK", decode_content=False, request_url=url)

    def _test_api_error(self, body):
        client = InfluxDBClient3(
            token='my-token',
            database='my-bucket',
            org='my-org'
        )
        client._write_api.rest_client.pool_manager.request \
            = mock.Mock(return_value=response.HTTPResponse(status=400,
                                                           reason='Bad Request',
                                                           body=body.encode()))
        client._write_api.write(record="data,foo=bar val=3.14")

    def test_default_headers(self):
        client = InfluxDBClient3(
            token='my-token',
            database='my-bucket',
            org='my-org'
        )
        write_api = client._write_api
        self.assertIsNotNone(write_api.default_header["User-Agent"])
        self.assertIsNotNone(write_api.default_header["Authorization"])
        self.assertEqual(f"{_package}/{VERSION}", write_api.default_header["User-Agent"])
        self.assertEqual("Token my-token", write_api.default_header["Authorization"])

    def test_api_error_cloud(self):
        response_body = '{"message": "parsing failed for write_lp endpoint"}'
        with self.assertRaises(InfluxDBError) as err:
            self._test_api_error(response_body)
        self.assertEqual('parsing failed for write_lp endpoint', err.exception.message)

    def test_api_error_oss_without_detail(self):
        response_body = '{"error": "parsing failed for write_lp endpoint"}'
        with self.assertRaises(InfluxDBError) as err:
            self._test_api_error(response_body)
        self.assertEqual('parsing failed for write_lp endpoint', err.exception.message)

    def test_api_error_oss_with_detail(self):
        response_body = ('{"error":"parsing failed for write_lp endpoint","data":{"error_message":"invalid field value '
                         'in line protocol for field \'val\' on line 1"}}')
        with self.assertRaises(InfluxDBError) as err:
            self._test_api_error(response_body)
        self.assertEqual("parsing failed for write_lp endpoint:\n\tinvalid field value in line protocol for field "
                         "'val' on line 1", err.exception.message)

    def test_api_error_unknown(self):
        response_body = '{"detail":"no info"}'
        with self.assertRaises(InfluxDBError) as err:
            self._test_api_error(response_body)
        self.assertEqual(response_body, err.exception.message)

    def test_api_error_v3_with_detail(self):
        cases = [
            # all details available
            (
                "two-line details",
                '{"error":"partial write of line protocol occurred","data":['
                '{"error_message":"invalid column type for column \'v\', expected iox::column_type::field::float, '
                'got iox::column_type::field::uinteger","line_number":2,"original_line":"**.DBG.remote_***"},'
                '{"error_message":"invalid column type for column \'v\', expected iox::column_type::field::float, '
                'got iox::column_type::field::uinteger","line_number":3,"original_line":"***.INF.remote_***"}'
                ']}',
                "partial write of line protocol occurred:\n"
                "\tline 2: invalid column type for column 'v', expected iox::column_type::field::float, "
                "got iox::column_type::field::uinteger (**.DBG.remote_***)\n"
                "\tline 3: invalid column type for column 'v', expected iox::column_type::field::float, "
                "got iox::column_type::field::uinteger (***.INF.remote_***)",
                True,
            ),
            # error_message only (no line_number/original_line)
            (
                "message-only detail",
                '{"error":"partial write of line protocol occurred","data":['
                '{"error_message":"only error message"}]}',
                "partial write of line protocol occurred:\n"
                "\tonly error message",
                True,
            ),
            # non-dict item in data list is skipped
            (
                "non-dict item skipped",
                '{"error":"partial write of line protocol occurred","data":[null,'
                '{"error_message":"bad line","line_number":2,"original_line":"bad lp"}]}',
                "partial write of line protocol occurred:\n"
                "\tline 2: bad line (bad lp)",
                True,
            ),
            # details empty -> return error_text
            (
                "no detail fields",
                '{"error":"partial write of line protocol occurred","data":[{"line_number":2}]}',
                "partial write of line protocol occurred:\n"
                "\t{\"line_number\":2}",
                False,
            ),
            # typed parse fails due line_number type -> raw fallback details
            (
                "textual line_number falls back to raw",
                '{"error":"partial write of line protocol occurred","data":'
                '[{"error_message":"bad line","line_number":"x","original_line":"bad lp"}]}',
                "partial write of line protocol occurred:\n"
                "\t{\"error_message\":\"bad line\",\"line_number\":\"x\",\"original_line\":\"bad lp\"}",
                False,
            ),
            # mixed valid + malformed in array -> raw fallback for whole array
            (
                "mixed array malformed item falls back to raw",
                '{"error":"partial write of line protocol occurred","data":'
                '[{"error_message":"bad line","line_number":2,"original_line":"bad lp"},1]}',
                "partial write of line protocol occurred:\n"
                "\t{\"error_message\":\"bad line\",\"line_number\":2,\"original_line\":\"bad lp\"}\n"
                "\t1",
                False,
            ),
            # data is not a dict when resolving fallback keys
            (
                "data not dict for fallback",
                '{"error":"data not list","data":"oops"}',
                "data not list",
                False,
            ),
            # typed object with empty message is dropped
            (
                "empty error_message in object",
                '{"error":"partial write of line protocol occurred","data":'
                '{"error_message":"","line_number":2,"original_line":"bad lp"}}',
                "partial write of line protocol occurred",
                False,
            ),
            # typed array parse fails, raw fallback skips null item
            (
                "raw fallback skips null details",
                '{"error":"partial write of line protocol occurred","data":'
                '[null,{"error_message":123}]}',
                "partial write of line protocol occurred:\n"
                "\t{\"error_message\":123}",
                False,
            ),
        ]
        for name, response_body, expected, is_partial in cases:
            with self.subTest(name):
                with self.assertRaises(InfluxDBError) as err:
                    self._test_api_error(response_body)
                self.assertEqual(expected, err.exception.message)
                if is_partial:
                    self.assertIsInstance(err.exception, InfluxDBPartialWriteError)
                    self.assertGreaterEqual(len(err.exception.line_errors), 1)
                else:
                    self.assertNotIsInstance(err.exception, InfluxDBPartialWriteError)

    def test_api_error_v3_parsing_failed_object_returns_partial_error(self):
        response_body = ('{"error":"parsing failed for write_lp endpoint","data":'
                         '{"error_message":"invalid field value","line_number":2,"original_line":"m,t=a f=bad"}}')
        with self.assertRaises(InfluxDBPartialWriteError) as err:
            self._test_api_error(response_body)
        self.assertEqual(1, len(err.exception.line_errors))
        self.assertEqual(2, err.exception.line_errors[0].line_number)

    def test_api_error_v3_partial_write_with_message_only_object_returns_partial_error(self):
        response_body = ('{"error":"partial write of line protocol occurred","data":'
                         '{"error_message":"only error message"}}')
        with self.assertRaises(InfluxDBPartialWriteError) as err:
            self._test_api_error(response_body)
        self.assertEqual(1, len(err.exception.line_errors))
        self.assertEqual(0, err.exception.line_errors[0].line_number)
        self.assertEqual("", err.exception.line_errors[0].original_line)

    def test_api_error_v3_partial_write_with_line_number_without_original_line(self):
        response_body = ('{"error":"partial write of line protocol occurred","data":'
                         '{"error_message":"invalid field value","line_number":2}}')
        with self.assertRaises(InfluxDBPartialWriteError) as err:
            self._test_api_error(response_body)
        self.assertEqual(1, len(err.exception.line_errors))
        self.assertEqual("partial write of line protocol occurred:\n\tline 2: invalid field value",
                         err.exception.message)

    def test_partial_write_from_response_guards(self):
        self.assertIsNone(InfluxDBPartialWriteError.from_response(None))

        empty_body = response.HTTPResponse(status=400, reason="Bad Request", body=b"")
        self.assertIsNone(InfluxDBPartialWriteError.from_response(empty_body))

        invalid_json = response.HTTPResponse(status=400, reason="Bad Request", body=b"{")
        self.assertIsNone(InfluxDBPartialWriteError.from_response(invalid_json))

        non_dict_json = response.HTTPResponse(status=400, reason="Bad Request", body=b"[]")
        self.assertIsNone(InfluxDBPartialWriteError.from_response(non_dict_json))

        object_without_typed_line_error = response.HTTPResponse(
            status=400,
            reason="Bad Request",
            body=b'{"error":"partial write of line protocol occurred","data":{"error_message":123}}',
        )
        self.assertIsNone(InfluxDBPartialWriteError.from_response(object_without_typed_line_error))

    def test_api_error_headers(self):
        body = '{"error": "test error"}'
        body_dic = json.loads(body)
        traceid = "123456789ABCDEF0"
        requestid = uuid.uuid4().__str__()

        client = InfluxDBClient3(
            token='my-token',
            database='my-bucket',
            org='my-org'
        )

        client._write_api.rest_client.pool_manager.request = mock.Mock(
            return_value=response.HTTPResponse(
                status=400,
                reason='Bad Request',
                headers={
                    'Trace-Id': traceid,
                    'Trace-Sampled': 'false',
                    'X-Influxdb-Request-Id': requestid,
                    'X-Influxdb-Build': 'Mock'
                },
                body=body.encode()
            )
        )
        with self.assertRaises(InfluxDBError) as err:
            client._write_api.write("TEST_ORG", "TEST_BUCKET", "data,foo=bar val=3.14")
        self.assertEqual(body_dic['error'], err.exception.message)
        headers = err.exception.getheaders()
        self.assertEqual(4, len(headers))
        self.assertEqual(headers['Trace-Id'], traceid)
        self.assertEqual(headers['Trace-Sampled'], 'false')
        self.assertEqual(headers['X-Influxdb-Request-Id'], requestid)
        self.assertEqual(headers['X-Influxdb-Build'], 'Mock')

    @mock.patch("urllib3._request_methods.RequestMethods.request",
                side_effect=mock_urllib3_timeout_request)
    def test_write_timeout(self, mock_request):
        host = "http://localhost:8181"
        timeout = 300
        client = InfluxDBClient3(
            host=host,
            token='my-token',
            database='my-bucket',
            org='my-org',
            write_timeout=timeout
        )

        with pytest.raises(ConnectTimeoutError):
            client._write_api.write("TEST_BUCKET", "TEST_ORG", "data,foo=bar val=3.14")
        self.assertEqual(0.3, self.received_timeout_total)
        self.received_timeout_total = None

    @mock.patch("urllib3._request_methods.RequestMethods.request",
                side_effect=mock_urllib3_timeout_request)
    def test_request_arg_timeout(self, mock_request):
        host = "http://localhost:8181"
        timeout = 300
        client = InfluxDBClient3(
            host=host,
            token='my-token',
            database='my-bucket',
            org='my-org',
            write_timeout=timeout
        )

        with pytest.raises(ConnectTimeoutError):
            client._write_api.write("TEST_ORG", "TEST_BUCKET", "data,foo=bar val=3.14",
                                    _request_timeout=100)
        self.assertEqual(0.1, self.received_timeout_total)
        self.received_timeout_total = None

    def test_should_gzip(self):
        client = InfluxDBClient3(
            host='http://localhost:8181',
            token='my-token',
            database='my-bucket',
            org='my-org'
        )
        write_api = client._write_api

        # Test when gzip is disabled
        self.assertFalse(write_api._should_gzip("test", enable_gzip=False, gzip_threshold=1))
        self.assertFalse(write_api._should_gzip("test", enable_gzip=False, gzip_threshold=10000))
        self.assertFalse(write_api._should_gzip("test", enable_gzip=False, gzip_threshold=None))

        # Test when enable_gzip is True
        self.assertTrue(write_api._should_gzip("test", enable_gzip=True, gzip_threshold=None))
        self.assertTrue(write_api._should_gzip("test", enable_gzip=True, gzip_threshold=1))
        self.assertFalse(write_api._should_gzip("test", enable_gzip=True, gzip_threshold=100000))

        # Test payload smaller than threshold
        self.assertFalse(write_api._should_gzip("test", enable_gzip=True, gzip_threshold=10000))

        # Test payload larger than threshold
        large_payload = "x" * 10000
        self.assertTrue(write_api._should_gzip(large_payload, enable_gzip=True, gzip_threshold=1000))

        # Test exact threshold match and less than threshold
        payload = "x" * 1000
        self.assertTrue(write_api._should_gzip(payload, enable_gzip=True, gzip_threshold=1000))

    def test_post_write_async_translates_exceptions(self):
        cases = [
            (
                "v2 on v3-only backend",
                True,
                response.HTTPResponse(status=405, reason="Method Not Allowed", body=b""),
                ApiException,
                "Server doesn't support the V2 API endpoint (/api/v2/write). "
                "Set use_v2_api=False to use the V3 API endpoint.",
            ),
            (
                "v3 on v2-only backend",
                False,
                response.HTTPResponse(status=405, reason="Method Not Allowed", body=b""),
                ApiException,
                "Server doesn't support the V3 API endpoint (/api/v3/write_lp). "
                "Set use_v2_api=True to use the V2 API endpoint.",
            ),
            (
                "v3 partial write response",
                False,
                response.HTTPResponse(
                    status=400,
                    reason="Bad Request",
                    body=(
                        b'{"error":"partial write of line protocol occurred","data":[{"error_message":"bad line",'
                        b'"line_number":2,"original_line":"home,room=Sunroom temp=\\"hi\\" 1735549200"}]}'
                    ),
                ),
                InfluxDBPartialWriteError,
                None,
            ),
        ]
        for name, use_v2_api, http_resp, expected_type, expected_message in cases:
            with self.subTest(name):
                client = InfluxDBClient3(
                    token='my-token',
                    database='my-bucket',
                    org='my-org'
                )
                write_api = client._write_api
                write_api.call_api = mock.Mock()
                thread = mock.Mock()
                thread.get.side_effect = ApiException(http_resp=http_resp)
                write_api.call_api.return_value = thread
                result = write_api._post_write(
                    org="TEST_ORG",
                    bucket="TEST_BUCKET",
                    body="home,room=Sunroom temp=96 1735545600",
                    precision='s',
                    accept_partial=False,
                    no_sync=False,
                    async_req=True,
                    _async_req=True,
                    use_v2_api=use_v2_api,
                )
                with self.assertRaises(expected_type) as err:
                    result.get()
                if expected_message:
                    self.assertEqual(expected_message, err.exception.message)
                    self.assertEqual(expected_message, err.exception.reason)
                else:
                    self.assertEqual(1, len(err.exception.line_errors))

    def test_post_write_async_translates_v3_unsupported(self):
        client = InfluxDBClient3(
            token='my-token',
            database='my-bucket',
            org='my-org',
        )

        write_api = client._write_api

        write_api.call_api = mock.AsyncMock(
            side_effect=ApiException(
                http_resp=response.HTTPResponse(status=405, reason="Method Not Allowed", body=b"")
            )
        )

        async def run():
            await write_api.post_write_async(
                "TEST_ORG",
                "TEST_BUCKET",
                "home,room=Sunroom temp=96 1735545600",
                use_v2_api=False,
            )

        with self.assertRaises(ApiException) as err:
            asyncio.run(run())

        expected = ("Server doesn't support the V3 API endpoint (/api/v3/write_lp). "
                    "Set use_v2_api=True to use the V2 API endpoint.")
        self.assertEqual(expected, err.exception.message)
