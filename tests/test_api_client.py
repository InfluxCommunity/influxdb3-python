import json
import unittest
import uuid
from unittest import mock
from urllib3 import response

from influxdb_client_3.write_client._sync.api_client import ApiClient
from influxdb_client_3.write_client.configuration import Configuration
from influxdb_client_3.write_client.client.exceptions import InfluxDBError
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
        self.assertEqual('invalid field value in line protocol for field \'val\' on line 1', err.exception.message)

    def test_api_error_unknown(self):
        response_body = '{"detail":"no info"}'
        with self.assertRaises(InfluxDBError) as err:
            self._test_api_error(response_body)
        self.assertEqual(response_body, err.exception.message)

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
