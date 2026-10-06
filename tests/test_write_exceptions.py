import http
import json
import unittest

from influxdb_client_3.exceptions import InfluxDBPartialWriteError, InfluxDBPartialWriteLineError
from influxdb_client_3.exceptions.write_exceptions import (
    ApiException,
    translate_write_exception,
)


class DummyHttpResponse:
    def __init__(self, status=200, reason="OK", data=None, headers=None):
        self.status = status
        self.reason = reason
        self.data = data
        self.headers = headers or {}

    def getheaders(self):
        return self.headers

    def getheader(self, name, default=None):
        return self.headers.get(name, default)


class TestWriteException(unittest.TestCase):

    def test_method_not_allowed_v3(self):
        exc = ApiException(status=http.HTTPStatus.METHOD_NOT_ALLOWED, reason="Method Not Allowed")
        result = translate_write_exception(exc, use_v2_api=False)

        self.assertIsInstance(result, ApiException)
        self.assertEqual(0, result.status)
        expected_msg = (
            "Server doesn't support the V3 API endpoint (/api/v3/write_lp). "
            "Set use_v2_api=True to use the V2 API endpoint."
        )
        self.assertEqual(expected_msg, result.message)
        self.assertEqual(expected_msg, result.reason)
        self.assertEqual((expected_msg,), result.args)

    def test_method_not_allowed_v2(self):
        exc = ApiException(status=http.HTTPStatus.METHOD_NOT_ALLOWED, reason="Method Not Allowed")
        result = translate_write_exception(exc, use_v2_api=True)

        self.assertIsInstance(result, ApiException)
        self.assertEqual(0, result.status)
        expected_msg = (
            "Server doesn't support the V2 API endpoint (/api/v2/write). "
            "Set use_v2_api=False to use the V3 API endpoint."
        )
        self.assertEqual(expected_msg, result.message)
        self.assertEqual(expected_msg, result.reason)
        self.assertEqual((expected_msg,), result.args)

    def test_status_zero_and_body_none(self):
        exc = ApiException(status=0, reason="Connection aborted")
        result = translate_write_exception(exc)

        self.assertIs(result, exc)
        self.assertEqual(0, result.status)
        self.assertEqual("Connection aborted", result.reason)

    def test_fallback_to_headers(self):
        header_keys = [
            ("X-Platform-Error-Code", "platform_error_code"),
            ("X-Influx-Error", "influx_error_header"),
            ("X-InfluxDb-Error", "influxdb_error_header"),
        ]
        for header_key, header_val in header_keys:
            with self.subTest(header_key=header_key):
                http_resp = DummyHttpResponse(
                    status=500,
                    reason="Internal Server Error",
                    data=b"raw body text",
                    headers={header_key: header_val},
                )
                exc = ApiException(http_resp=http_resp)
                result = translate_write_exception(exc)

                self.assertIs(result, exc)
                self.assertEqual(header_val, result.message)

    def test_fallback_header_precedence(self):
        http_resp = DummyHttpResponse(
            status=500,
            reason="Internal Server Error",
            data=b"raw body text",
            headers={
                "X-Platform-Error-Code": "platform_code",
                "X-Influx-Error": "influx_error",
                "X-InfluxDb-Error": "influxdb_error",
            },
        )
        exc = ApiException(http_resp=http_resp)
        result = translate_write_exception(exc)

        self.assertIs(result, exc)
        self.assertEqual("platform_code", result.message)

    def test_fallback_to_raw_body_when_no_headers_and_invalid_json(self):
        http_resp = DummyHttpResponse(
            status=500,
            reason="Internal Server Error",
            data=b"raw plain text error",
            headers={},
        )
        exc = ApiException(http_resp=http_resp)
        result = translate_write_exception(exc)

        self.assertIs(result, exc)
        self.assertEqual(b"raw plain text error", result.message)

    def test_fallback_to_status_reason_when_no_headers_and_empty_body(self):
        for data in [None, ""]:
            with self.subTest(data=data):
                http_resp = DummyHttpResponse(
                    status=500,
                    reason="Internal Server Error",
                    data=data,
                    headers={},
                )
                exc = ApiException(http_resp=http_resp)
                result = translate_write_exception(exc)

                self.assertIs(result, exc)
                self.assertEqual("Internal Server Error", result.message)

    def test_fallback_when_json_is_not_dict_or_has_no_error_or_message(self):
        cases = [
            b'""',
            b'"just a string"',
            b'123',
            b'[1, 2, 3]',
            b'{}',
            b'{"status": "failed"}',
            b'{"other_key": 42}',
        ]
        for body in cases:
            with self.subTest(body=body):
                http_resp = DummyHttpResponse(
                    status=500,
                    reason="Internal Server Error",
                    data=body,
                    headers={},
                )
                exc = ApiException(http_resp=http_resp)
                result = translate_write_exception(exc)

                self.assertIs(result, exc)
                self.assertEqual(body, result.message)

    def test_v3_message_only(self):
        body = json.dumps({"message": "table 'cpu' not found"}).encode("utf-8")
        http_resp = DummyHttpResponse(status=404, reason="Not Found", data=body)
        exc = ApiException(http_resp=http_resp)
        result = translate_write_exception(exc)

        self.assertIs(result, exc)
        self.assertEqual("table 'cpu' not found", result.message)

    def test_v3_code_and_message(self):
        body = json.dumps({"code": "not_found", "message": "table 'cpu' not found"}).encode("utf-8")
        http_resp = DummyHttpResponse(status=404, reason="Not Found", data=body)
        exc = ApiException(http_resp=http_resp)
        result = translate_write_exception(exc)

        self.assertIs(result, exc)
        self.assertEqual("not_found: table 'cpu' not found", result.message)

    def test_error_without_data(self):
        body = json.dumps({"error": "syntax error on token"}).encode("utf-8")
        http_resp = DummyHttpResponse(status=400, reason="Bad Request", data=body)
        exc = ApiException(http_resp=http_resp)
        result = translate_write_exception(exc)

        self.assertIs(result, exc)
        self.assertEqual("syntax error on token", result.message)

    def test_object_data_error_without_line_number(self):
        body = json.dumps({
            "error": "write failed",
            "data": {
                "error_message": "type conflict for field 'temp'"
            }
        }).encode("utf-8")
        http_resp = DummyHttpResponse(status=400, reason="Bad Request", data=body)
        exc = ApiException(http_resp=http_resp)
        result = translate_write_exception(exc)

        self.assertIs(result, exc)
        self.assertEqual("write failed:\n\ttype conflict for field 'temp'", result.message)

    def test_object_data_error_with_line_number_no_original_line(self):
        body = json.dumps({
            "error": "write failed",
            "data": {
                "line_number": 4,
                "error_message": "type conflict for field 'temp'"
            }
        }).encode("utf-8")
        http_resp = DummyHttpResponse(status=400, reason="Bad Request", data=body)
        exc = ApiException(http_resp=http_resp)
        result = translate_write_exception(exc)

        self.assertIs(result, exc)
        self.assertEqual("write failed:\n\tline 4: type conflict for field 'temp'", result.message)

    def test_object_data_error_with_line_number_and_original_line(self):
        body = json.dumps({
            "error": "write failed",
            "data": {
                "line_number": 4,
                "error_message": "type conflict for field 'temp'",
                "original_line": "cpu,tag=1 temp=10 123456789"
            }
        }).encode("utf-8")
        http_resp = DummyHttpResponse(status=400, reason="Bad Request", data=body)
        exc = ApiException(http_resp=http_resp)
        result = translate_write_exception(exc)

        self.assertIs(result, exc)
        self.assertEqual(
            "write failed:\n\tline 4: type conflict for field 'temp' (cpu,tag=1 temp=10 123456789)",
            result.message
        )

    def test_object_data_error_without_error_message(self):
        body = json.dumps({
            "error": "write failed",
            "data": {
                "line_number": 4
            }
        }).encode("utf-8")
        http_resp = DummyHttpResponse(status=400, reason="Bad Request", data=body)
        exc = ApiException(http_resp=http_resp)
        result = translate_write_exception(exc)

        self.assertIs(result, exc)
        self.assertEqual("write failed", result.message)

    def test_partial_write_error_all_typed_details(self):
        body = json.dumps({
            "error": "partial write of line protocol occurred",
            "data": [
                {
                    "line_number": 1,
                    "error_message": "type mismatch",
                    "original_line": "m,t=a f=1"
                },
                {
                    "line_number": 2,
                    "error_message": "invalid timestamp"
                },
                {
                    "error_message": "general line error"
                }
            ]
        }).encode("utf-8")
        http_resp = DummyHttpResponse(status=400, reason="Bad Request", data=body)
        exc = ApiException(http_resp=http_resp)
        result = translate_write_exception(exc, use_v2_api=False, accept_partial=True)

        self.assertIsInstance(result, InfluxDBPartialWriteError)
        expected_msg = (
            "partial write of line protocol occurred:\n"
            "\tline 1: type mismatch (m,t=a f=1)\n"
            "\tline 2: invalid timestamp\n"
            "\tgeneral line error"
        )
        self.assertEqual(expected_msg, result.message)
        self.assertEqual(3, len(result.line_errors))
        self.assertEqual(
            [
                InfluxDBPartialWriteLineError(1, "type mismatch", "m,t=a f=1"),
                InfluxDBPartialWriteLineError(2, "invalid timestamp", None),
                InfluxDBPartialWriteLineError(None, "general line error", None),
            ],
            result.line_errors
        )
        self.assertEqual(http_resp, result.response)

    def test_partial_write_error_untyped_details_fallback(self):
        body = json.dumps({
            "error": "partial write of line protocol occurred",
            "data": [
                {
                    "line_number": "not_an_int",
                    "error_message": "type mismatch",
                    "original_line": "m,t=a f=1"
                },
                None,
                "null",
                {"line_number": 2},
                123
            ]
        }).encode("utf-8")
        http_resp = DummyHttpResponse(status=400, reason="Bad Request", data=body)
        exc = ApiException(http_resp=http_resp)
        result = translate_write_exception(exc, use_v2_api=False, accept_partial=True)

        self.assertIsInstance(result, InfluxDBPartialWriteError)
        expected_msg = (
            "partial write of line protocol occurred:\n"
            '\t{"line_number":"not_an_int","error_message":"type mismatch","original_line":"m,t=a f=1"}\n'
            '\t{"line_number":2}\n'
            '\t123'
        )
        self.assertEqual(expected_msg, result.message)

    def test_partial_write_skipped_when_accept_partial_is_false(self):
        body = json.dumps({
            "error": "partial write of line protocol occurred",
            "data": [
                {
                    "line_number": 1,
                    "error_message": "type mismatch",
                    "original_line": "m,t=a f=1"
                }
            ]
        }).encode("utf-8")
        http_resp = DummyHttpResponse(status=400, reason="Bad Request", data=body)
        exc = ApiException(http_resp=http_resp)
        result = translate_write_exception(exc, use_v2_api=False, accept_partial=False)

        self.assertIsInstance(result, ApiException)
        self.assertEqual("partial write of line protocol occurred", result.message)

    def test_partial_write_skipped_when_use_v2_api_is_true(self):
        body = json.dumps({
            "error": "partial write of line protocol occurred",
            "data": [
                {
                    "line_number": 1,
                    "error_message": "type mismatch",
                    "original_line": "m,t=a f=1"
                }
            ]
        }).encode("utf-8")
        http_resp = DummyHttpResponse(status=400, reason="Bad Request", data=body)
        exc = ApiException(http_resp=http_resp)
        result = translate_write_exception(exc, use_v2_api=True, accept_partial=True)

        self.assertIsInstance(result, ApiException)
        self.assertEqual("partial write of line protocol occurred", result.message)

    def test_partial_write_skipped_when_status_is_not_400(self):
        body = json.dumps({
            "error": "partial write of line protocol occurred",
            "data": [
                {
                    "line_number": 1,
                    "error_message": "type mismatch",
                    "original_line": "m,t=a f=1"
                }
            ]
        }).encode("utf-8")
        http_resp = DummyHttpResponse(status=500, reason="Internal Server Error", data=body)
        exc = ApiException(http_resp=http_resp)
        result = translate_write_exception(exc, use_v2_api=False, accept_partial=True)

        self.assertIsInstance(result, ApiException)
        self.assertEqual("partial write of line protocol occurred", result.message)
