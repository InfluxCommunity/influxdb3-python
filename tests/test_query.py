import asyncio
import time
import unittest
import os
import json
from unittest.mock import Mock, ANY

from pyarrow.flight import (
    FlightClient,
    Ticket
)

from influxdb_client_3 import InfluxDBClient3
from influxdb_client_3.query.query_api import QueryApiOptionsBuilder, QueryApi
from influxdb_client_3.version import USER_AGENT
from tests.util import asyncio_run

from tests.util.mocks import (
    ConstantData,
    ConstantFlightServer,
    ConstantFlightServerDelayed,
    HeaderCheckFlightServer,
    HeaderCheckServerMiddlewareFactory,
    NoopAuthHandler,
    get_req_headers,
    set_req_headers
)


def case_insensitive_header_lookup(headers, lkey):
    """Lookup the value of a given key in the given headers.
       The lkey is case-insensitive.
    """
    for key in headers:
        if key.lower() == lkey.lower():
            return headers.get(key)


def test_influx_default_query_headers():
    with HeaderCheckFlightServer(
            auth_handler=NoopAuthHandler(),
            middleware={"check": HeaderCheckServerMiddlewareFactory()}) as server:
        global _req_headers
        set_req_headers({})
        client = InfluxDBClient3(
            host=f'http://localhost:{server.port}',
            org='test_org',
            databse='test_db',
            token='TEST_TOKEN'
        )
        client.query('SELECT * FROM test')
        _req_headers = get_req_headers()
        assert len(_req_headers) > 0
        assert _req_headers['authorization'][0] == "Bearer TEST_TOKEN"
        assert _req_headers['user-agent'][0].find(USER_AGENT) > -1
        set_req_headers({})


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

    def test_multiple_flight_client_options(self):

        q_opts = QueryApiOptionsBuilder().flight_client_options({
            'generic_options': [('optA', 'A in options')]
        }).build()

        q_api = QueryApi(
            connection_string="grpc+tls://my-server.org",
            token='my_token',
            flight_client_options={"generic_options": [('opt1', 'opt1 in args')]},
            proxy=None,
            options=q_opts
        )

        assert ('opt1', 'opt1 in args') in q_api._flight_client_options['generic_options']
        assert ('optA', 'A in options') in q_api._flight_client_options['generic_options']
        assert (('grpc.secondary_user_agent', USER_AGENT) in
                q_api._flight_client_options['generic_options'])

    def test_override_secondary_user_agent_args(self):
        q_api = QueryApi(
            connection_string="grpc+tls://my-server.org",
            token='my_token',
            flight_client_options={"generic_options": [('grpc.secondary_user_agent', 'my_custom_user_agent')]},
            proxy=None,
            options=None
        )

        assert ('grpc.secondary_user_agent', 'my_custom_user_agent') in q_api._flight_client_options['generic_options']
        assert not (('grpc.secondary_user_agent', USER_AGENT) in
                    q_api._flight_client_options['generic_options'])

    def test_secondary_user_agent_in_options(self):
        q_opts = QueryApiOptionsBuilder().flight_client_options({
            'generic_options': [
                ('optA', 'A in options'),
                ('grpc.secondary_user_agent', 'my_custom_user_agent')
            ]
        }).build()

        q_api = QueryApi(
            connection_string="grpc+tls://my-server.org",
            token='my_token',
            flight_client_options=None,
            proxy=None,
            options=q_opts
        )

        assert ('optA', 'A in options') in q_api._flight_client_options['generic_options']
        assert ('grpc.secondary_user_agent', 'my_custom_user_agent') in q_api._flight_client_options['generic_options']
        assert (('grpc.secondary_user_agent', USER_AGENT) not in
                q_api._flight_client_options['generic_options'])

    def test_prepare_query(self):
        set_req_headers({})
        token = 'my_token'
        q_api = QueryApi(
            connection_string="grpc+tls://my-server.org",
            token=token,
            flight_client_options={"generic_options": [('Foo', 'Bar')]},
            proxy=None,
            options=None
        )

        query = "SELECT * FROM sensors"
        language = "sql"
        database = "my_database"

        ticket, options = q_api._prepare_query(query=query,
                                               language=language,
                                               database=database)
        tkt = json.loads(ticket.ticket.decode('utf-8'))
        assert tkt['database'] == database
        assert tkt['sql_query'] == query
        assert tkt['query_type'] == language

        with HeaderCheckFlightServer(
                auth_handler=NoopAuthHandler(),
                middleware={"check": HeaderCheckServerMiddlewareFactory()}) as server:
            with FlightClient(('localhost', server.port)) as client:
                client.do_get(ticket, options)
                _req_headers = get_req_headers()
                assert _req_headers['authorization'] == [f"Bearer {token}"]
                set_req_headers({})

    @asyncio_run
    async def test_query_async_pandas(self):
        with ConstantFlightServer() as server:
            connection_string = f"grpc://localhost:{server.port}"
            token = "my_token"
            database = "my_database"
            q_api = QueryApi(
                connection_string=connection_string,
                token=token,
                flight_client_options={"generic_options": [('Foo', 'Bar')]},
                proxy=None,
                options=None
            )

            query = "SELECT * FROM data"
            pndf = await q_api.query_async(query, "sql", "pandas", database)

            cd = ConstantData()
            numpy_array = pndf.T.to_numpy()
            tuples = []
            for n in range(len(numpy_array[0])):
                tuples.append((numpy_array[0][n], numpy_array[1][n], numpy_array[2][n]))

            for constant in cd.to_tuples():
                assert constant in tuples

            assert ('sql_query', query, -1.0) in tuples
            assert ('database', database, -1.0) in tuples
            assert ('query_type', 'sql', -1.0) in tuples

    @asyncio_run
    async def test_query_async_table(self):
        with ConstantFlightServer() as server:
            connection_string = f"grpc://localhost:{server.port}"
            token = "my_token"
            database = "my_database"
            q_api = QueryApi(
                connection_string=connection_string,
                token=token,
                flight_client_options={"generic_options": [('Foo', 'Bar')]},
                proxy=None,
                options=None
            )
            query = "SELECT * FROM data"
            table = await q_api.query_async(query, "sql", "", database)

            cd = ConstantData()

            result_list = table.to_pylist()
            for item in cd.to_list():
                assert item in result_list

            assert {'data': 'database', 'reference': 'my_database', 'value': -1.0} in result_list
            assert {'data': 'sql_query', 'reference': 'SELECT * FROM data', 'value': -1.0} in result_list
            assert {'data': 'query_type', 'reference': 'sql', 'value': -1.0} in result_list

    @asyncio_run
    async def test_query_async_delayed(self):
        events = dict()
        with ConstantFlightServerDelayed(delay=1) as server:
            connection_string = f"grpc://localhost:{server.port}"
            token = "my_token"
            database = "my_database"
            q_api = QueryApi(
                connection_string=connection_string,
                token=token,
                flight_client_options={"generic_options": [('Foo', 'Bar')]},
                proxy=None,
                options=None
            )
            query = "SELECT * FROM data"

            # coroutine to handle query_async
            async def local_query(query_api):
                events['query_start'] = time.time_ns()
                t_result = await query_api.query_async(query, "sql", "", database)
                # t_result = query_api.query(query, "sql", "", database)
                events['query_result'] = time.time_ns()
                return t_result

            # second coroutine to run in "parallel"
            async def fibo(iters):
                events['fibo_start'] = time.time_ns()
                await asyncio.sleep(0.5)
                n0 = 1
                n1 = 1
                result = n1 + n0
                for _ in range(iters):
                    n0 = n1
                    n1 = result
                    result = n1 + n0
                events['fibo_end'] = time.time_ns()
                return result

            results = await asyncio.gather(local_query(q_api), fibo(50))

            table = results[0]
            fibo_num = results[1]

            # verify fibo calculation
            assert fibo_num == 53316291173

            # verify constant data
            cd = ConstantData()

            result_list = table.to_pylist()
            for item in cd.to_list():
                assert item in result_list

            # verify that fibo coroutine was processed while query_async was processing
            # i.e. query call does not block event_loop
            # fibo started after query_async
            assert events['query_start'] < events['fibo_start'], (f"query_start: {events['query_start']} should start "
                                                                  f"before fibo_start: {events['fibo_start']}")
            # fibo started before query_async ends - i.e. query_async did not block it
            assert events['query_result'] > events['fibo_start'], (f"query_result: {events['query_result']} should "
                                                                   f"occur after fibo_start: {events['fibo_start']}")

            # fibo ended before query_async
            assert events['query_result'] > events['fibo_end'], (f"query_result: {events['query_result']} should occur "
                                                                 f"after fibo_end: {events['fibo_end']}")
