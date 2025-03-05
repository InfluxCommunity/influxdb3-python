import unittest
import struct
import os
from unittest.mock import Mock, ANY

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
from influxdb_client_3.query.query_api import QueryApiOptionsBuilder, QueryApi
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
    def __init__(self, token, *args, **kwargs):
        super().__init__(*args, **kwargs)
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

    sample_cert = """-----BEGIN CERTIFICATE-----
MIIFDTCCAvWgAwIBAgIUYzpfisy9xLrhiZd+D9vOdzC3+iswDQYJKoZIhvcNAQEL
BQAwFjEUMBIGA1UEAwwLdGVzdGhvc3QuaW8wHhcNMjUwMjI4MTM1NTMyWhcNMzUw
MjI2MTM1NTMyWjAWMRQwEgYDVQQDDAt0ZXN0aG9zdC5pbzCCAiIwDQYJKoZIhvcN
AQEBBQADggIPADCCAgoCggIBAN1lwqXYP8UMvjb56SpUEj2OpoEDRfLeWrEiHkOl
xoymvJGaXZNEpDXo2TTdysCoYWEjz9IY6GlqSo2Yssf5BZkQwMOw7MdyRwCigzrh
OAKbyCfsvEgfNFrXEdSDpaxW++5SToeErudYXc+sBfnI1NB4W3GBGqqIvx8fqaB3
1EU9ql2sKKxI0oYIQD/If9rQEyLFKeWdD8iT6YST1Vugkvd34NPmaqV5+pjdSb4z
a8olavwUoslqFUeILqIq+WZZbOlgCcJYKcBAmELRnsxGaABRtMwMZx+0D+oKo4Kl
QQtOcER+RHkBHyYFghZIBnzudfbP9NadknOz3AilJbJolXfXJqeQhRD8Ob49kkhe
OwjAppHnaZGWjYZMLIfnwwXBwkS7bSwF16Wot83cpL46Xvg6xcl12An4JaoF798Q
cXyYrWCgvbqjVR7694gxqLGzk138AKTDSbER1h1rfqCqkk7soE0oWCs7jiCk2XvD
49qVfHtd50KYJ4/yP1XL0PmLL0Hw1kvOxLVkFENc1zkoYXJRt2Ec6j9dajmGlsFn
0bLLap6UIlIGQFuvcLf4bvsIi9FICy2jBjaIdM4UAWbReG+52+180HEleAwi5bAN
HY61WVXc4X+N0E2y8HWc1QaRioU7R4XZ5HXKs7OTWkKFZUU2JDFHAKdiiAU78qLU
7GApAgMBAAGjUzBRMB0GA1UdDgQWBBT2vPFo0mzh9ls4xJUiAgSK+B5LpTAfBgNV
HSMEGDAWgBT2vPFo0mzh9ls4xJUiAgSK+B5LpTAPBgNVHRMBAf8EBTADAQH/MA0G
CSqGSIb3DQEBCwUAA4ICAQC4TJNPx476qhiMi8anISv9lo9cnLju+qNhcz7wupBH
3Go6bVQ7TCbSt2QpAyY64mdnRqHsXeGvZXCnabOpeKRDeAPBtRjc6yNKuXybqFtn
W3PZEs/OYc659TUA+MoBzSXYStN9yiiYXyVFqVn+Rw6kM9tKh0GgAU7f5P+8IGuR
gXJbCjkbdJO7JUiVGEEmkjUHyqFxMHaZ8V6uazs52qIFyt7OYQTeV9HdoW8D9vAt
GfzYwzRDzbsZeIJqqDzLe7NOyxEyqZHCbtNpGcOyaLOl7ZBS52WsqaUZtL+9PjqD
2TWj4WUFkOWQpTvWKHqM6//Buv4GjnTBShQKm+h+rxcGkdRMF6/sKwxPbr39P3RJ
TMfJA3u5UuowT44VaA2jkQzqIbxH9+3EA+0qPbqPJchOSr0pHSncqvR9FYcr7ayN
b6UDFnjeliyEqqksUO0arbvaO9FfB0kH8lU1NOKaQNO++Xj69GZMC6s721cNdad0
qqcdtyXWeOBBchguYDrSUIgLnUTHEwwzOmcNQ36hO5eX282BJy3ZLT3JU6MJopjz
vkbDDAxSrpZMcaoAWSrxgJAETeYiO4YbfORIzPkwdUkEIr6XY02Pi7MdkDGQ5hiB
TavA8+oXRa4b9BR3bCWcg8S/t4uOTTLkeTcQbONPh5A5IRySLCU+CwqB+/+VlO8X
Aw==
-----END CERTIFICATE-----"""

    def setUp(self):
        self.client = InfluxDBClient3(
            host="localhost",
            org="my_org",
            database="my_db",
            token="my_token"
        )

    def test_query_without_parameters(self):
        mock_do_get = Mock()
        self.client._query_api._do_get = mock_do_get

        self.client.query('SELECT * FROM measurement')

        expected_ticket = Ticket(
            '{"database": "my_db", '
            '"sql_query": "SELECT * FROM measurement", '
            '"query_type": "sql"}'.encode('utf-8')
        )

        mock_do_get.assert_called_once_with(expected_ticket, ANY)

    def test_query_with_parameters(self):
        mock_do_get = Mock()
        self.client._query_api._do_get = mock_do_get

        self.client.query('SELECT * FROM measurement WHERE time > $time', query_parameters={"time": "2021-01-01"})

        expected_ticket = Ticket(
            '{"database": "my_db", '
            '"sql_query": "SELECT * FROM measurement WHERE time > $time", '
            '"query_type": "sql", "params": {"time": "2021-01-01"}}'.encode('utf-8')
        )

        mock_do_get.assert_called_once_with(expected_ticket, ANY)

    def test_query_proxy_base_client(self):
        test_proxy = "http://testproxy:5432"
        client = InfluxDBClient3(
            host="http://localhost:8443",
            token="my-token",
            org="my-org",
            database="my-database",
            proxy=test_proxy
        )

        assert client._query_api._proxy == test_proxy
        assert ('grpc.http_proxy', test_proxy) in\
               client._query_api._flight_client_options.get('generic_options')

    def create_cert_file(self, file_name):
        f = open(file_name, "w")
        f.write(self.sample_cert)
        f.close()

    def remove_cert_file(self, file_name):
        os.remove(file_name)

    def test_query_api_options_builder(self):
        proxy_name = "http://my.proxy.org"
        cert_file = "cert_test.pem"
        self.create_cert_file(cert_file)
        builder = QueryApiOptionsBuilder()
        options = builder.proxy(proxy_name)\
            .root_certs(cert_file)\
            .tls_verify(False)\
            .build()

        try:
            assert options.tls_root_certs.decode('utf-8') == self.sample_cert
            assert not options.tls_verify
            assert options.proxy == proxy_name
        finally:
            self.remove_cert_file(cert_file)

    def test_query_client_with_options(self):
        connection = "grpc+tls://localhost:9999"
        token = "my_token"
        proxy_name = "http://my.proxy.org"
        cert_file = "cert_test.pem"
        private_key = 'our_key'
        cert_chain = 'mTLS_explicit_chain'
        self.create_cert_file(cert_file)
        test_flight_client_options = {'private_key': private_key, 'cert_chain': cert_chain}
        options = QueryApiOptionsBuilder()\
            .proxy(proxy_name) \
            .root_certs(cert_file) \
            .tls_verify(False) \
            .flight_client_options(test_flight_client_options) \
            .build()

        client = QueryApi(connection,
                          token,
                          None,
                          None,
                          options
                          )

        try:
            assert client._token == token
            assert client._flight_client_options['tls_root_certs'].decode('utf-8') == self.sample_cert
            assert client._flight_client_options['private_key'] == private_key
            assert client._flight_client_options['cert_chain'] == cert_chain
            assert client._proxy == proxy_name
            fc_opts = client._flight_client_options
            assert dict(fc_opts['generic_options'])['grpc.secondary_user_agent'].startswith('influxdb3-python/')
            assert dict(fc_opts['generic_options'])['grpc.http_proxy'] == proxy_name
        finally:
            self.remove_cert_file(cert_file)

    def test_client_with_ssl_args(self):
        cert_name = "cert-test.pem"
        self.create_cert_file(cert_name)
        proxy = "http://localhost:9999"
        local_client = InfluxDBClient3(
            host="localhost",
            org="my_org",
            database="my_db",
            token="my_token",
            proxy=proxy,
            ssl_ca_cert=cert_name,
            verify_ssl=False
        )

        try:
            qapi = local_client._query_api
            fc_opts = qapi._flight_client_options
            assert qapi._proxy == proxy
            assert fc_opts['tls_root_certs'].decode('utf-8') == self.sample_cert
            assert fc_opts['disable_server_verification']
            assert dict(fc_opts['generic_options'])['grpc.secondary_user_agent'].startswith('influxdb3-python/')
            assert dict(fc_opts['generic_options'])['grpc.http_proxy'] == proxy
        finally:
            self.remove_cert_file(cert_name)
