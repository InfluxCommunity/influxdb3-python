import asyncio
import http
import json
import unittest
import uuid
from dataclasses import dataclass, field
from typing import Optional, List
from unittest import mock

import pytest
from urllib3 import response
from urllib3.exceptions import ConnectTimeoutError

from influxdb_client_3 import InfluxDBClient3, InfluxDBError
from influxdb_client_3.exceptions import InfluxDBPartialWriteError, InfluxDBPartialWriteLineError
from influxdb_client_3.version import VERSION
from influxdb_client_3.write_client.write_exceptions import ApiException

_package = "influxdb3-python"
_sentHeaders = {}


@dataclass
class TestCase:
    __test__ = False
    name: str
    status_code: int
    response_body: str
    content_type: Optional[str] = None
    use_v2_api: bool = False
    accept_partial: bool = False
    expected_msg: str = ""
    expect_partial: bool = False
    expected_lines: List[InfluxDBPartialWriteLineError] = field(default_factory=list)

    def __str__(self):
        return self.name


POINTS = (
    "home,room=Sunroom temp=96 1735545600\n"
    'home,room=Sunroom temp="hi" 1735545610\n'
    "home,room=Sunroom temp=88i 1735545620"
)
REJECTED_LINE = 'home,room=Sunroom temp="hi" 1735545610'
REJECTED_LINE_JSON = 'home,room=Sunroom temp=\\"hi\\" 1735545610'
LINE_ERROR = (
    "invalid column type for column 'temp', expected "
    "iox::column_type::field::float, got iox::column_type::field::string"
)

TEST_CASES = [
    TestCase(
        name="V3 accept partial with renamed error and non-empty array",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body=(
            f'{{"error":"write completed with rejected rows","data":['
            f'{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}'
            f"]}}"
        ),
        accept_partial=True,
        expected_msg=f"write completed with rejected rows:\n\tline 2: {LINE_ERROR} ({REJECTED_LINE})",
        expect_partial=True,
        expected_lines=[
            InfluxDBPartialWriteLineError(
                error_message=LINE_ERROR,
                line_number=2,
                original_line=REJECTED_LINE,
            )
        ],
    ),
    TestCase(
        name="V3 accept partial without content type",
        status_code=http.client.BAD_REQUEST,
        content_type=None,
        response_body=(
            f'{{"error":"write completed with rejected rows","data":['
            f'{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}'
            f"]}}"
        ),
        accept_partial=True,
        expected_msg=f"write completed with rejected rows:\n\tline 2: {LINE_ERROR} ({REJECTED_LINE})",
        expect_partial=True,
        expected_lines=[
            InfluxDBPartialWriteLineError(
                error_message=LINE_ERROR,
                line_number=2,
                original_line=REJECTED_LINE,
            )
        ],
    ),
    TestCase(
        name="V3 accept partial with malformed non-empty array",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body=(
            f'{{"error":"write completed with rejected rows","data":['
            f'{{"line_number":"invalid","original_line":"{REJECTED_LINE_JSON}"}}'
            f"]}}"
        ),
        accept_partial=True,
        expected_msg=f'write completed with rejected rows:\n\t{{"line_number":"invalid","original_line":'
                     f'"{REJECTED_LINE_JSON}"}}',
        expect_partial=True,
        expected_lines=[],
    ),
    TestCase(
        name="V3 accept partial with mixed primitive and typed entries",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body=(
            f'{{"error":"write completed with rejected rows","data":['
            f'1,{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}'
            f"]}}"
        ),
        accept_partial=True,
        expected_msg=(
            f"write completed with rejected rows:\n\t1\n\t"
            f'{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}'
        ),
        expect_partial=True,
        expected_lines=[
            InfluxDBPartialWriteLineError(
                error_message=LINE_ERROR,
                line_number=2,
                original_line=REJECTED_LINE,
            )
        ],
    ),
    TestCase(
        name="V3 accept partial with string entries",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body=f'{{"error":"write completed with rejected rows","data":["{REJECTED_LINE_JSON}"]}}',
        accept_partial=True,
        expected_msg=f'write completed with rejected rows:\n\t"{REJECTED_LINE_JSON}"',
        expect_partial=True,
        expected_lines=[],
    ),
    TestCase(
        name="V3 accept partial with error message only",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body=f'{{"error":"write completed with rejected rows",'
                      f'"data":[{{"error_message":"{LINE_ERROR}"}}]}}',
        accept_partial=True,
        expected_msg=f"write completed with rejected rows:\n\t{LINE_ERROR}",
        expect_partial=True,
        expected_lines=[
            InfluxDBPartialWriteLineError(
                error_message=LINE_ERROR,
                line_number=None,
                original_line=None,
            )
        ],
    ),
    TestCase(
        name="V3 accept partial with line number but no original line",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body=f'{{"error":"write completed with rejected rows","data":[{{"error_message":"{LINE_ERROR}",'
                      f'"line_number":2}}]}}',
        accept_partial=True,
        expected_msg=f"write completed with rejected rows:\n\tline 2: {LINE_ERROR}",
        expect_partial=True,
        expected_lines=[
            InfluxDBPartialWriteLineError(
                error_message=LINE_ERROR,
                line_number=2,
                original_line=None,
            )
        ],
    ),
    TestCase(
        name="V3 accept partial with entry missing error message",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body=f'{{"error":"write completed with rejected rows",'
                      f'"data":[{{"line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}]}}',
        accept_partial=True,
        expected_msg=f'write completed with rejected rows:\n\t{{"line_number":2,'
                     f'"original_line":"{REJECTED_LINE_JSON}"}}',
        expect_partial=True,
        expected_lines=[],
    ),
    TestCase(
        name="V3 accept partial with empty array",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body='{"error":"write failed","data":[]}',
        accept_partial=True,
        expected_msg="write failed",
        expect_partial=False,
    ),
    TestCase(
        name="V3 accept partial with object details remains generic",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body=(
            f'{{"error":"line protocol parsing error","data":'
            f'{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}}}'
        ),
        accept_partial=True,
        expected_msg=f"line protocol parsing error:\n\tline 2: {LINE_ERROR} ({REJECTED_LINE})",
        expect_partial=False,
    ),
    TestCase(
        name="V3 reject partial with object details",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body=(
            f'{{"error":"line protocol parsing error","data":'
            f'{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}}}'
        ),
        accept_partial=False,
        expected_msg=f"line protocol parsing error:\n\tline 2: {LINE_ERROR} ({REJECTED_LINE})",
        expect_partial=False,
    ),
    TestCase(
        name="V2 never returns partial write error",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body=(
            f'{{"error":"partial write of line protocol occurred","data":['
            f'{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}'
            f"]}}"
        ),
        use_v2_api=True,
        accept_partial=True,
        expected_msg="partial write of line protocol occurred",
        expect_partial=False,
    ),
    TestCase(
        name="V3 non-400 never returns partial write error",
        status_code=http.client.INTERNAL_SERVER_ERROR,
        content_type="application/json",
        response_body=(
            f'{{"error":"partial write of line protocol occurred","data":['
            f'{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}'
            f"]}}"
        ),
        accept_partial=True,
        expected_msg="partial write of line protocol occurred",
        expect_partial=False,
    ),
    TestCase(
        name="V3 scalar data remains generic",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body='{"error":"write failed","data":"invalid"}',
        accept_partial=True,
        expected_msg="write failed",
        expect_partial=False,
    ),
    TestCase(
        name="V3 empty object data remains generic",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body='{"error":"write failed","data":{}}',
        accept_partial=True,
        expected_msg="write failed",
        expect_partial=False,
    ),
    TestCase(
        name="V3 null data remains generic",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body='{"error":"write failed","data":null}',
        accept_partial=True,
        expected_msg="write failed",
        expect_partial=False,
    ),
    TestCase(
        name="V3 malformed JSON preserves raw response",
        status_code=http.client.BAD_REQUEST,
        content_type="application/json",
        response_body='{"error":"write failed"',
        accept_partial=True,
        expected_msg='{"error":"write failed"',
        expect_partial=False,
    ),
]


class WriteApiTests(unittest.TestCase):
    received_timeout_total = None

    def mock_urllib3_timeout_request(method,
                                     url,
                                     **urlopen_kw):
        if urlopen_kw.get('timeout', None) is not None:
            WriteApiTests.received_timeout_total = urlopen_kw['timeout'].total
            raise ConnectTimeoutError()

        return response.HTTPResponse(status=200, version=4, reason="OK", decode_content=False, request_url=url)

    def _test_api_error(self, body, header=None, accept_partial=None, use_v2_api=None):
        client = InfluxDBClient3(
            host='http://localhost:8181',
            token='my-token',
            database='my-bucket',
            org='my-org'
        )
        if body is not None:
            body = body.encode()

        client._write_api.rest_client.pool_manager.request \
            = mock.Mock(return_value=response.HTTPResponse(status=400,
                                                           headers=header or {},
                                                           reason='Bad Request',
                                                           body=body))
        client._write_api.write(record="data,foo=bar val=3.14", accept_partial=accept_partial, use_v2_api=use_v2_api)

    def test_default_headers(self):
        client = InfluxDBClient3(
            host='http://localhost:8181',
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
                False,
                1
            ),
            # error_message only (no line_number/original_line)
            (
                "message-only detail",
                '{"error":"partial write of line protocol occurred","data":['
                '{"error_message":"only error message"}]}',
                "partial write of line protocol occurred:\n"
                "\tonly error message",
                True,
                False,
                1
            ),
            # non-dict item in data list is skipped
            (
                "non-dict item skipped",
                '{"error":"partial write of line protocol occurred","data":[null,'
                '{"error_message":"bad line","line_number":2,"original_line":"bad lp"}]}',
                "partial write of line protocol occurred:\n"
                "\t{\"error_message\":\"bad line\",\"line_number\":2,\"original_line\":\"bad lp\"}",
                True,
                False,
                1
            ),
            # details empty -> return error_text
            (
                "no detail fields",
                '{"error":"partial write of line protocol occurred","data":[{"line_number":2}]}',
                "partial write of line protocol occurred:\n"
                "\t{\"line_number\":2}",
                True,
                False,
                0
            ),
            # typed parse fails due line_number type -> raw fallback details
            (
                "textual line_number falls back to raw",
                '{"error":"partial write of line protocol occurred","data":'
                '[{"error_message":"bad line","line_number":"x","original_line":"bad lp"}]}',
                "partial write of line protocol occurred:\n"
                "\t{\"error_message\":\"bad line\",\"line_number\":\"x\",\"original_line\":\"bad lp\"}",
                True,
                False,
                0
            ),
            # mixed valid + malformed in array -> raw fallback for whole array
            (
                "mixed array malformed item falls back to raw",
                '{"error":"partial write of line protocol occurred","data":'
                '[{"error_message":"bad line","line_number":2,"original_line":"bad lp"},1]}',
                "partial write of line protocol occurred:\n"
                "\t{\"error_message\":\"bad line\",\"line_number\":2,\"original_line\":\"bad lp\"}\n"
                "\t1",
                True,
                False,
                1
            ),
            # data is not a dict when resolving fallback keys
            (
                "data not dict for fallback",
                '{"error":"data not list","data":"oops"}',
                "data not list",
                False,
                True,
                0
            ),
            # typed object with empty message is dropped
            (
                "empty error_message in object",
                '{"error":"parsing failed for write_lp endpoint","data":'
                '{"error_message":"","line_number":2,"original_line":"bad lp"}}',
                "parsing failed for write_lp endpoint",
                False,
                True,
                0
            ),
            # typed array parse fails, raw fallback skips null item
            (
                "raw fallback skips null details",
                '{"error":"partial write of line protocol occurred","data":'
                '[null,{"error_message":123}]}',
                "partial write of line protocol occurred:\n"
                "\t{\"error_message\":123}",
                True,
                False,
                1
            ),
        ]
        for name, response_body, expected, is_partial, use_v2_api, expected_line_error_count in cases:
            with self.subTest(name):
                if is_partial:
                    with self.assertRaises(InfluxDBPartialWriteError) as err:
                        self._test_api_error(body=response_body, accept_partial=is_partial, use_v2_api=use_v2_api)
                    self.assertIsInstance(err.exception, InfluxDBPartialWriteError)
                    self.assertGreaterEqual(len(err.exception.line_errors), expected_line_error_count)
                else:
                    with self.assertRaises(ApiException) as err:
                        self._test_api_error(body=response_body, accept_partial=is_partial, use_v2_api=use_v2_api)
                self.assertEqual(expected, err.exception.message)

    def test_api_error_v3_parsing_failed_object_returns_error(self):
        response_body = ('{"error":"parsing failed for write_lp endpoint","data":'
                         '{"error_message":"invalid field value","line_number":2,"original_line":"m,t=a f=bad"}}')
        with self.assertRaises(ApiException) as err:
            self._test_api_error(response_body)
        self.assertEqual('parsing failed for write_lp endpoint:\n\tline 2: invalid field value (m,t=a f=bad)',
                         err.exception.message)

    def test_api_error_v3_write_with_message_only_object_returns(self):
        response_body = ('{"error":"parsing failed for write_lp endpoint","data":'
                         '{"error_message":"only error message"}}')
        with self.assertRaises(ApiException) as err:
            self._test_api_error(response_body)
        self.assertEqual("parsing failed for write_lp endpoint:\n\tonly error message", err.exception.message)

    def test_api_error_v3_write_with_line_number_without_original_line(self):
        response_body = ('{"error":"parsing failed for write_lp endpoint","data":'
                         '{"error_message":"invalid field value","line_number":2}}')
        with self.assertRaises(ApiException) as err:
            self._test_api_error(response_body)
        self.assertEqual("parsing failed for write_lp endpoint:\n\tline 2: invalid field value",
                         err.exception.message)

    def test_fallback_header_or_body(self):
        for body in ["{err", "[]", "{}"]:
            for is_partial_write in [False, True]:
                # Fallback to header message
                with self.assertRaises(InfluxDBError) as err:
                    header = {"X-Influx-Error": "not used"}
                    self._test_api_error(
                        body=body,
                        header=header,
                        accept_partial=is_partial_write,
                        use_v2_api=False
                    )
                self.assertEqual(header["X-Influx-Error"], err.exception.message)

                # Fallback to raw body
                with self.assertRaises(InfluxDBError) as err:
                    self._test_api_error(
                        body=body,
                        accept_partial=is_partial_write,
                        use_v2_api=False
                    )
                self.assertEqual(body, err.exception.message)

    def test_fallback_status_code_msg(self):
        for body in ["", None]:
            for is_partial_write in [False, True]:
                with self.assertRaises(InfluxDBError) as err:
                    self._test_api_error(
                        body=body,
                        accept_partial=is_partial_write,
                        use_v2_api=False
                    )
                self.assertEqual('Bad Request', err.exception.message)

    def test_api_error_headers(self):
        body = '{"error": "test error"}'
        body_dic = json.loads(body)
        traceid = "123456789ABCDEF0"
        requestid = uuid.uuid4().__str__()

        client = InfluxDBClient3(
            host='http://localhost:8181',
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
            client._write_api.write("TEST_BUCKET", "TEST_ORG", "data,foo=bar val=3.14")
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
            client._write_api.write("TEST_BUCKET", "TEST_ORG", "data,foo=bar val=3.14",
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
                False,
                response.HTTPResponse(status=405, reason="Method Not Allowed", body=b""),
                ApiException,
                "Server doesn't support the V2 API endpoint (/api/v2/write). "
                "Set use_v2_api=False to use the V3 API endpoint.",
            ),
            (
                "v3 on v2-only backend",
                False,
                False,
                response.HTTPResponse(status=405, reason="Method Not Allowed", body=b""),
                ApiException,
                "Server doesn't support the V3 API endpoint (/api/v3/write_lp). "
                "Set use_v2_api=True to use the V2 API endpoint.",
            ),
            (
                "v3 partial write response",
                False,
                True,
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
        for name, use_v2_api, accept_partial, http_resp, expected_type, expected_message in cases:
            with self.subTest(name):
                client = InfluxDBClient3(
                    host='http://localhost:8181',
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
                    accept_partial=accept_partial,
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
            host='http://localhost:8181',
            token='my-token',
            database='my-bucket',
            org='my-org',
        )

        write_api = client._write_api

        write_api.rest_client.request = mock.Mock(
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

    def test_write_error_classification(self):
        for tc in TEST_CASES:
            with self.subTest(tc.name):
                headers = {"Content-Type": tc.content_type} if tc.content_type is not None else {}

                client = InfluxDBClient3(
                    host="http://localhost:8086",
                    token="token",
                    database="database",
                )
                client._write_api.rest_client.pool_manager.request = mock.Mock(
                    return_value=response.HTTPResponse(
                        status=tc.status_code,
                        headers=headers,
                        body=tc.response_body.encode("utf-8"),
                    )
                )

                expected_exc = InfluxDBPartialWriteError if tc.expect_partial else InfluxDBError
                with self.assertRaises(expected_exc) as cm:
                    client.write(
                        record=POINTS,
                        use_v2_api=tc.use_v2_api,
                        accept_partial=tc.accept_partial,
                    )

                err = cm.exception
                self.assertEqual(tc.expected_msg, err.message)
                if tc.expect_partial:
                    self.assertEqual(tc.expected_lines, err.line_errors)

    def test_translate_write_exception_direct(self):
        client = InfluxDBClient3(
            host="http://localhost:8086",
            token="token",
            database="database",
        )
        write_api = client._write_api

        # 405 Method Not Allowed - use_v2_api=True
        exc = ApiException(http_resp=response.HTTPResponse(status=405, reason="Method Not Allowed", body=b""))
        translated = write_api._translate_write_exception(exc, use_v2_api=True, accept_partial=False)
        self.assertIsInstance(translated, ApiException)
        self.assertEqual(0, translated.status)
        self.assertIn("Server doesn't support the V2 API endpoint", translated.message)

        # 405 Method Not Allowed - use_v2_api=False
        exc = ApiException(http_resp=response.HTTPResponse(status=405, reason="Method Not Allowed", body=b""))
        translated = write_api._translate_write_exception(exc, use_v2_api=False, accept_partial=False)
        self.assertIsInstance(translated, ApiException)
        self.assertEqual(0, translated.status)
        self.assertIn("Server doesn't support the V3 API endpoint", translated.message)

        # Non-JSON body fallback to reason
        exc = ApiException(http_resp=response.HTTPResponse(status=500, reason="Internal Server Error", body=b"plain error"))
        translated = write_api._translate_write_exception(exc, use_v2_api=False, accept_partial=False)
        self.assertEqual(b"plain error", translated.message)

        # JSON with message field
        exc = ApiException(http_resp=response.HTTPResponse(status=400, reason="Bad Request", body=b'{"message": "custom error"}'))
        translated = write_api._translate_write_exception(exc, use_v2_api=False, accept_partial=False)
        self.assertEqual("custom error", translated.message)

        # Partial write error
        partial_body = b'{"error":"write failed","data":[{"error_message":"invalid value","line_number":1,"original_line":"m v=1"}]}'
        exc = ApiException(http_resp=response.HTTPResponse(status=400, reason="Bad Request", body=partial_body))
        translated = write_api._translate_write_exception(exc, use_v2_api=False, accept_partial=True)
        self.assertIsInstance(translated, InfluxDBPartialWriteError)
        self.assertEqual(1, len(translated.line_errors))
