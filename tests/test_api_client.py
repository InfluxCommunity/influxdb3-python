import json
import unittest
import uuid
from unittest import mock

import pytest
from urllib3 import response
from urllib3.exceptions import ConnectTimeoutError

from influxdb_client_3.write_client._sync.api_client import ApiClient
from influxdb_client_3.write_client.configuration import Configuration
from influxdb_client_3.exceptions import InfluxDBError
from influxdb_client_3.write_client.service import WriteService
from influxdb_client_3.version import VERSION

_package = "influxdb3-python"
_sentHeaders = {}


def mock_rest_request(method,
                      url,
                      query_params=None,
                      headers=None,
                      body=None,
                      post_params=None,
                      _preload_content=True,
                      _request_timeout=None,
                      **urlopen_kw):
    class MockResponse:
        def __init__(self, data, status_code):
            self.data = data
            self.status_code = status_code

        def data(self):
            return self.data

    global _sentHeaders
    _sentHeaders = headers

    return MockResponse(None, 200)


class ApiClientTests(unittest.TestCase):

    received_timeout_total = None

    def mock_urllib3_timeout_request(method,
                                     url,
                                     body,
                                     headers,
                                     **urlopen_kw):
        if urlopen_kw.get('timeout', None) is not None:
            ApiClientTests.received_timeout_total = urlopen_kw['timeout'].total
            raise ConnectTimeoutError()

        return response.HTTPResponse(status=200, version=4, reason="OK", decode_content=False, request_url=url)

    def test_default_headers(self):
        global _package
        conf = Configuration()
        client = ApiClient(conf,
                           header_name="Authorization",
                           header_value="Bearer TEST_TOKEN")
        self.assertIsNotNone(client.default_headers["User-Agent"])
        self.assertIsNotNone(client.default_headers["Authorization"])
        self.assertEqual(f"{_package}/{VERSION}", client.default_headers["User-Agent"])
        self.assertEqual("Bearer TEST_TOKEN", client.default_headers["Authorization"])

    @mock.patch("influxdb_client_3.write_client._sync.rest.RESTClientObject.request",
                side_effect=mock_rest_request)
    def test_call_api(self, mock_post):
        global _package
        global _sentHeaders
        _sentHeaders = {}

        conf = Configuration()
        client = ApiClient(conf,
                           header_name="Authorization",
                           header_value="Bearer TEST_TOKEN")
        service = WriteService(client)
        service.post_write("TEST_ORG", "TEST_BUCKET", "data,foo=bar val=3.14")
        self.assertEqual(4, len(_sentHeaders.keys()))
        self.assertIsNotNone(_sentHeaders["Accept"])
        self.assertEqual("application/json", _sentHeaders["Accept"])
        self.assertIsNotNone(_sentHeaders["Content-Type"])
        self.assertEqual("text/plain", _sentHeaders["Content-Type"])
        self.assertIsNotNone(_sentHeaders["Authorization"])
        self.assertEqual("Bearer TEST_TOKEN", _sentHeaders["Authorization"])
        self.assertIsNotNone(_sentHeaders["User-Agent"])
        self.assertEqual(f"{_package}/{VERSION}", _sentHeaders["User-Agent"])

    def _test_api_error(self, body):
        conf = Configuration()
        client = ApiClient(conf)
        client.rest_client.pool_manager.request \
            = mock.Mock(return_value=response.HTTPResponse(status=400,
                                                           reason='Bad Request',
                                                           body=body.encode()))
        service = WriteService(client)
        service.post_write("TEST_ORG", "TEST_BUCKET", "data,foo=bar val=3.14")

    def test_api_error_formats(self):
        """Test various error response formats are parsed correctly using data-driven approach."""
        test_cases = [
            {
                'name': 'cloud',
                'response_body': '{"code": "internal", "message": "parsing failed for write_lp endpoint"}',
                'expected_message': 'parsing failed for write_lp endpoint',
            },
            {
                'name': 'oss_without_detail',
                'response_body': '{"error": "parsing failed for write_lp endpoint"}',
                'expected_message': 'parsing failed for write_lp endpoint',
            },
            {
                'name': 'unknown',
                'response_body': '{"detail":"no info"}',
                'expected_message': '{"detail":"no info"}',
            },
            {
                'name': 'oss_with_line_details',
                'response_body': ('{"error":"partial write of line protocol occurred","data":['
                                  '{"error_message":"A generic parsing error occurred: TakeWhile1","line_number":2,'
                                  '"original_line":"temperatureroom=room"},'
                                  '{"error_message":"invalid column type for column \'value\', expected '
                                  'iox::column_type::field::float, got iox::column_type::field::integer",'
                                  '"line_number":4,"original_line":"temperature,room=roo"}]}'
                                  ),
                'expected_message_contains': [
                    'partial write of line protocol occurred',
                    'Line 2:',
                    'A generic parsing error occurred: TakeWhile1',
                    'Original: temperatureroom=room',
                    'Line 4:',
                    'invalid column type',
                    'Original: temperature,room=roo',
                ],
            },
        ]

        for test_case in test_cases:
            with self.subTest(format=test_case['name']):
                with self.assertRaises(InfluxDBError) as err:
                    self._test_api_error(test_case['response_body'])

                # Check exact message match
                if 'expected_message' in test_case:
                    self.assertEqual(test_case['expected_message'], err.exception.message)

                # Check message contains all expected strings
                if 'expected_message_contains' in test_case:
                    for expected_str in test_case['expected_message_contains']:
                        self.assertIn(expected_str, err.exception.message)

    def test_api_error_headers(self):
        body = '{"error": "test error"}'
        body_dic = json.loads(body)
        conf = Configuration()
        local_client = ApiClient(conf)
        traceid = "123456789ABCDEF0"
        requestid = uuid.uuid4().__str__()

        local_client.rest_client.pool_manager.request = mock.Mock(
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
            service = WriteService(local_client)
            service.post_write("TEST_ORG", "TEST_BUCKET", "data,foo=bar val=3.14")
        self.assertEqual(body_dic['error'], err.exception.message)
        headers = err.exception.getheaders()
        self.assertEqual(4, len(headers))
        self.assertEqual(headers['Trace-Id'], traceid)
        self.assertEqual(headers['Trace-Sampled'], 'false')
        self.assertEqual(headers['X-Influxdb-Request-Id'], requestid)
        self.assertEqual(headers['X-Influxdb-Build'], 'Mock')

    @mock.patch("urllib3._request_methods.RequestMethods.request",
                side_effect=mock_urllib3_timeout_request)
    def test_request_config_timeout(self, mock_request):
        conf = Configuration()
        conf.host = "http://localhost:8181"
        conf.timeout = 300
        local_client = ApiClient(conf)
        service = WriteService(local_client)
        with pytest.raises(ConnectTimeoutError):
            service.post_write("TEST_ORG", "TEST_BUCKET", "data,foo=bar val=3.14",
                               _preload_content=False)
        self.assertEqual(0.3, self.received_timeout_total)
        self.received_timeout_total = None

    @mock.patch("urllib3._request_methods.RequestMethods.request",
                side_effect=mock_urllib3_timeout_request)
    def test_request_arg_timeout(self, mock_request):
        conf = Configuration()
        conf.host = "http://localhost:8181"
        conf.timeout = 300
        local_client = ApiClient(conf)
        service = WriteService(local_client)
        with pytest.raises(ConnectTimeoutError):
            service.post_write("TEST_ORG", "TEST_BUCKET", "data,foo=bar val=3.14",
                               _request_timeout=100, _preload_content=False)
        self.assertEqual(0.1, self.received_timeout_total)
        self.received_timeout_total = None

    def test_should_gzip(self):
        # Test when gzip is disabled
        self.assertFalse(ApiClient.should_gzip("test", enable_gzip=False, gzip_threshold=1))
        self.assertFalse(ApiClient.should_gzip("test", enable_gzip=False, gzip_threshold=10000))
        self.assertFalse(ApiClient.should_gzip("test", enable_gzip=False, gzip_threshold=None))

        # Test when enable_gzip is True
        self.assertTrue(ApiClient.should_gzip("test", enable_gzip=True, gzip_threshold=None))
        self.assertTrue(ApiClient.should_gzip("test", enable_gzip=True, gzip_threshold=1))
        self.assertFalse(ApiClient.should_gzip("test", enable_gzip=True, gzip_threshold=100000))

        # Test payload smaller than threshold
        self.assertFalse(ApiClient.should_gzip("test", enable_gzip=True, gzip_threshold=10000))

        # Test payload larger than threshold
        large_payload = "x" * 10000
        self.assertTrue(ApiClient.should_gzip(large_payload, enable_gzip=True, gzip_threshold=1000))

        # Test exact threshold match and less than threshold
        payload = "x" * 1000
        self.assertTrue(ApiClient.should_gzip(payload, enable_gzip=True, gzip_threshold=1000))
