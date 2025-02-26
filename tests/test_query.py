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
MIIDUzCCAjugAwIBAgIUZB55ULutbc9gy6xLp1BkTQU7siowDQYJKoZIhvcNAQEL
BQAwNjE0MDIGA1UEAwwraW5mbHV4ZGIzLWNsdXN0ZXJlZC1zd2FuLmJyYW1ib3Jh
LnpvbmEtYi5ldTAeFw0yNTAyMTgxNTIyMTJaFw0yNjAyMTgxNTIyMTJaMDYxNDAy
BgNVBAMMK2luZmx1eGRiMy1jbHVzdGVyZWQtc3dhbi5icmFtYm9yYS56b25hLWIu
ZXUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCugeNrx0ZfyyP8H4e0
zDSkKWnEXlVdjMi+ZSHhMbjvvqMkUQGLc/W59AEmMJ0Uiljka9d+F7jdu+oqDq9p
4kGPhO3Oh7zIG0IGbncj8AwIXMGDNkNyL8s7C1+LoYotlSWDpWwkEKXUeAzdqS63
CSJFqSJM2dss8qe9BpM6zHWJAKS1I30QT3SXQFEsF5m2F62dXCEEI6pO7jlik8/w
aI47dTM20QyimVzea48SC/ELO/T4AjbmMeBGlTyCm39KOElOKRTJvB4KESEWaL3r
EvPZbTh+72PUyrjxiDa56+RmtDPo7EN3uxuRVFX/HWiNnFk7orQLKZg5Kr8wE46R
KmVvAgMBAAGjWTBXMDYGA1UdEQQvMC2CK2luZmx1eGRiMy1jbHVzdGVyZWQtc3dh
bi5icmFtYm9yYS56b25hLWIuZXUwHQYDVR0OBBYEFH8et6JCzGD7Ny84aNRtq5Nj
hvS/MA0GCSqGSIb3DQEBCwUAA4IBAQCuDwARea/Xr3+hmte9A0H+XB8wMPAJ64e8
QA0qi0oy0gGdLfQHhsBWWmKSYLv7HygTNzb+7uFOTtq1UPLt18F+POPeLIj74QZV
z89Pbo1TwUMzQ2pgbu0yRvraXIpqXGrPm5GWYp5mopX0rBWKdimbmEMkhZA0sVeH
IdKIRUY6EyIVG+Z/nbuVqUlgnIWOMp0yg4RRC91zHy3Xvykf3Vai25H/jQpa6cbU
//MIodzUIqT8Tja5cHXE51bLdUkO1rtNKdM7TUdjzkZ+bAOpqKl+c0FlYZI+F7Ly
+MdCcNgKFc8o8jGiyP6uyAJeg+tSICpFDw00LyuKmU62c7VKuyo7
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

        print(f"\nDEBUG options {vars(options)}")
        try:
            assert(options.tls_root_certs.decode('utf-8') == self.sample_cert)
            assert(options.tls_verify == False)
            assert(options.proxy == proxy_name)
        finally:
            self.remove_cert_file(cert_file)

    def test_query_client_with_options(self):
        connection = "grpc+tls://localhost:9999"
        token = "my_token"
        proxy_name = "http://my.proxy.org"
        cert_file = "cert_test.pem"
        self.create_cert_file(cert_file)
        options = QueryApiOptionsBuilder()\
            .proxy(proxy_name) \
            .root_certs(cert_file) \
            .tls_verify(False) \
            .build()

        client = QueryApi(connection,
                          token,
                          None,
                          None,
                          options
                          )

        print(f"\nDEBUG client {vars(client)}")
        try:
            assert(client._token == token)
            assert(client._flight_client_options['tls_root_certs'].decode('utf-8') == self.sample_cert)
            assert(client._proxy == proxy_name)
            # print(f"DEBUG client._flight_client_options['generic_options'] {dict(client._flight_client_options['generic_options'])['grpc.secondary_user_agent']}")
            assert(dict(client._flight_client_options['generic_options'])['grpc.secondary_user_agent'].startswith('influxdb3-python/'))
            assert(dict(client._flight_client_options['generic_options'])['grpc.http_proxy'] == proxy_name)
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
            ssl_ca_cert = cert_name,
            verify_ssl = False
        )

        try:
            qapi = local_client._query_api
            fc_opts = qapi._flight_client_options
            assert(qapi._proxy == proxy)
            assert(fc_opts['tls_root_certs'].decode('utf-8') == self.sample_cert)
            assert(fc_opts['disable_server_verification'] == True)
            assert(dict(fc_opts['generic_options'])['grpc.secondary_user_agent'].startswith('influxdb3-python/'))
            assert(dict(fc_opts['generic_options'])['grpc.http_proxy'] == proxy)
        finally:
            self.remove_cert_file(cert_name)
