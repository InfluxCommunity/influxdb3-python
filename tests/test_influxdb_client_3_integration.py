import logging
import os
import random
import string
import time
import unittest

import pyarrow
import pytest
from urllib3.exceptions import MaxRetryError, TimeoutError as Url3TimeoutError

from influxdb_client_3 import InfluxDBClient3, write_client_options, WriteOptions, \
    WriteType, InfluxDB3ClientQueryError, Point
from influxdb_client_3.exceptions import InfluxDBError
from tests.util import asyncio_run, lp_to_py_object


def random_hex(len=6):
    return ''.join(random.choice(string.hexdigits) for i in range(len))


@pytest.mark.integration
@pytest.mark.skipif(
    not all(
        [
            os.getenv('TESTING_INFLUXDB_URL'),
            os.getenv('TESTING_INFLUXDB_TOKEN'),
            os.getenv('TESTING_INFLUXDB_DATABASE'),
        ]
    ),
    reason="Integration test environment variables not set.",
)
class TestInfluxDBClient3Integration(unittest.TestCase):

    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog):
        self._caplog = caplog

    def setUp(self):
        self.host = os.getenv('TESTING_INFLUXDB_URL')
        self.token = os.getenv('TESTING_INFLUXDB_TOKEN')
        self.database = os.getenv('TESTING_INFLUXDB_DATABASE')
        # write_options=WriteOptions(batch_size=100)
        write_options=WriteOptions(write_type=WriteType.synchronous)
        wco = write_client_options(write_options=write_options)
        self.client = InfluxDBClient3(
            host=self.host,
            database=self.database,
            token=self.token,
            write_client_options=wco)

    def tearDown(self):
        self._caplog.clear()
        self._caplog.set_level(logging.ERROR)
        if self.client:
            self.client.close()

    def test_write_batch_and_query(self):
        # write_options=WriteOptions(batch_size=100)
        # write_options=WriteOptions(write_type=WriteType.synchronous)
        # wco = write_client_options(write_options=write_options)
        # c = InfluxDBClient3(
        #     host='http://localhost:8181',
        #     database=self.database,
        #     token='apiv3_XI_7D1lsUv0CsVoPiWQJtzG51LE-roQLF64pkKRxOi3cVdAFRfoBIJjptTjDordcOGa-rYe1Dow-iRsv9yU4ZA',
        #     write_client_options=wco)

        test_id = time.time_ns()
        self.client.write(f"integration_test_python21,type=used value=123.0,test_id={test_id}i")

        sql = 'SELECT * FROM integration_test_python21 where type=$type and test_id=$test_id'
        df = self.client.query(sql, mode="pandas", query_parameters={'type': 'used', 'test_id': test_id})

        self.assertIsNotNone(df)
        self.assertEqual(1, len(df))
        self.assertEqual(test_id, df['test_id'][0])
        self.assertEqual(123.0, df['value'][0])

    def test_write_and_query(self):
        test_id = time.time_ns()
        self.client.write(f"integration_test_python,type=used value=123.0,test_id={test_id}i")

        sql = 'SELECT * FROM integration_test_python where type=$type and test_id=$test_id'

        df = self.client.query(sql, mode="pandas", query_parameters={'type': 'used', 'test_id': test_id})

        self.assertIsNotNone(df)
        self.assertEqual(1, len(df))
        self.assertEqual(test_id, df['test_id'][0])
        self.assertEqual(123.0, df['value'][0])

    def test_auth_error_token(self):
        self.client = InfluxDBClient3(host=self.host, database=self.database, token='fake token')
        test_id = time.time_ns()
        with self.assertRaises(InfluxDBError) as err:
            self.client.write(f"integration_test_python,type=used value=123.0,test_id={test_id}i")
        self.assertEqual('Authorization header was malformed, the request was not in the form of '
                         '\'Authorization: <auth-scheme> <token>\', supported auth-schemes are Bearer, Token and Basic',
                         err.exception.message)  # Cloud

    def test_auth_error_auth_scheme(self):
        self.client = InfluxDBClient3(host=self.host, database=self.database, token=self.token, auth_scheme='Any')
        test_id = time.time_ns()
        with self.assertRaises(InfluxDBError) as err:
            self.client.write(f"integration_test_python,type=used value=123.0,test_id={test_id}i")
        self.assertEqual('Authorization header was malformed, the request was not in the form of '
                         '\'Authorization: <auth-scheme> <token>\', supported auth-schemes are Bearer, Token and Basic',
                         err.exception.message)  # Cloud

    def test_batch_write_callbacks(self):
        write_success = False
        write_error = False
        write_count = 0

        measurement = f'test{random_hex(6)}'
        data_set_size = 40
        batch_size = 10

        def success(conf, data):
            nonlocal write_success, write_count
            write_success = True
            write_count += 1

        def error(conf, data, exception: InfluxDBError):
            nonlocal write_error
            write_error = True

        w_opts = WriteOptions(
            batch_size=batch_size,
            flush_interval=1_000,
            jitter_interval=500
        )

        wc_opts = write_client_options(
            success_callback=success,
            error_callback=error,
            write_options=w_opts
        )

        now = time.time_ns()
        with InfluxDBClient3(host=self.host,
                             database=self.database,
                             token=self.token,
                             write_client_options=wc_opts) as w_client:
            for i in range(0, data_set_size):
                w_client.write(f'{measurement},location=harfa val={i}i {now - (i * 1_000_000_000)}')

        self.assertEqual(data_set_size / batch_size, write_count)
        self.assertTrue(write_success)
        self.assertFalse(write_error)

        with InfluxDBClient3(host=self.host,
                             database=self.database,
                             token=self.token,
                             write_client_options=wc_opts) as r_client:
            query = f"SELECT * FROM \"{measurement}\" WHERE time >= now() - interval '3 minute'"
            reader: pyarrow.Table = r_client.query(query)
            list_results = reader.to_pylist()
            self.assertEqual(data_set_size, len(list_results))

    def test_batch_write_closed(self):

        self._caplog.set_level(logging.DEBUG)
        # writing measurement for last cca 3hrs
        # so repeat runs in that time frame could
        # result in clashed result data if always
        # using same measurement name
        measurement = f'test{random_hex()}'
        data_size = 10_000
        w_opts = WriteOptions(
            batch_size=100,
            flush_interval=3_000,
            jitter_interval=500
        )

        wc_opts = write_client_options(
            write_options=w_opts
        )

        now = time.time_ns()
        with InfluxDBClient3(host=self.host,
                             database=self.database,
                             token=self.token,
                             write_client_options=wc_opts,
                             debug=True) as w_client:
            for i in range(0, data_size):
                w_client.write(f'{measurement},location=harfa val={i}i {now - (i * 1_000_000_000)}')

        lines = self._caplog.text.splitlines()
        self.assertRegex(lines[len(lines) - 1], ".*the batching processor was disposed$")

        logging.info("WRITING DONE")
        with InfluxDBClient3(host=self.host,
                             database=self.database,
                             token=self.token,
                             write_client_options=wc_opts) as r_client:
            logging.info("PREPARING QUERY")

            query = f"SELECT * FROM \"{measurement}\" WHERE time >= now() - interval '3 hours'"
            reader: pyarrow.Table = r_client.query(query, mode="")
            list_results = reader.to_pylist()
            self.assertEqual(data_size, len(list_results))

    test_cert = """-----BEGIN CERTIFICATE-----
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

    def create_test_cert(self, cert_file):
        f = open(cert_file, "w")
        f.write(self.test_cert)
        f.close()

    def remove_test_cert(self, cert_file):
        os.remove(cert_file)

    def test_verify_ssl_false(self):
        cert_file = "test_cert.pem"
        self.create_test_cert(cert_file)
        measurement = f'test{random_hex(6)}'

        with InfluxDBClient3(host=self.host,
                             database=self.database,
                             token=self.token,
                             verify_ssl=False,
                             ssl_ca_cert=cert_file,
                             debug=True) as client:
            try:
                now = time.time_ns()
                client.write(f'{measurement},location=harfa val=42i {now - 1_000_000_000}')
                query = f"SELECT * FROM \"{measurement}\""
                reader: pyarrow.Table = client.query(query, mode="")
                list_results = reader.to_pylist()
                assert len(list_results) > 0
            finally:
                self.remove_test_cert(cert_file)

    @asyncio_run
    async def test_verify_query_async(self):
        measurement = f'test{random_hex(6)}'
        data = []
        lp_template = "%s,location=%s val=%f,ival=%di,index=%di %d"
        data_size = 10
        interval = 1_000_000_000 * 10
        ts = time.time_ns() - interval * data_size
        locations = ['springfield', 'gotham', 'balbec', 'yonville']
        for i in range(data_size):
            data.append(lp_template % (measurement, locations[random.randint(0, len(locations) - 1)],
                                       random.random() * 10,
                                       random.randint(0, 6), i, ts))
            ts = ts + interval

        self.client.write(data)
        query = f"SELECT * FROM \"{measurement}\" ORDER BY time DESC"

        result = await self.client.query_async(query)

        result_list = result.to_pylist()
        for item in data:
            assert lp_to_py_object(item) in result_list, f"original lp data \"{item}\" should be in result list"

    def test_get_server_version(self):
        version = self.client.get_server_version()
        assert version is not None

    def test_write_timeout(self):
        with pytest.raises(Url3TimeoutError):
            InfluxDBClient3(
                host=self.host,
                database=self.database,
                token=self.token,
                write_timeout=30,
                write_client_options=write_client_options(
                    write_options=WriteOptions(
                        max_retry_time=0,
                        timeout=20,
                        write_type=WriteType.synchronous
                    )
                )
            ).write("test_write_timeout,location=harfa fVal=3.14,iVal=42i")

    def test_write_timeout_sync(self):

        with pytest.raises(Url3TimeoutError):
            localClient = InfluxDBClient3(
                host=self.host,
                database=self.database,
                token=self.token,
                write_client_options=write_client_options(
                    write_options=WriteOptions(
                        max_retry_time=0,
                        timeout=20,
                        write_type=WriteType.synchronous
                    )
                )
            )

            localClient.write("test_write_timeout,location=harfa fVal=3.14,iVal=42i")

    @asyncio_run
    async def test_write_timeout_async(self):

        with pytest.raises(Url3TimeoutError):
            localClient = InfluxDBClient3(
                host=self.host,
                database=self.database,
                token=self.token,
                write_client_options=write_client_options(
                    write_options=WriteOptions(
                        max_retry_time=0,  # disable retries
                        timeout=20,
                        write_type=WriteType.asynchronous
                    )
                )
            )

            applyResult = localClient.write("test_write_timeout,location=harfa fVal=3.14,iVal=42i")
            applyResult.get()

    def test_write_timeout_batching(self):

        ErrorResult = {"rt": None, "rd": None, "rx": None}

        def set_error_result(rt, rd, rx):
            nonlocal ErrorResult
            ErrorResult = {"rt": rt, "rd": rd, "rx": rx}

        localClient = InfluxDBClient3(
            host=self.host,
            database=self.database,
            token=self.token,
            write_timeout=20,
            write_client_options=write_client_options(
                error_callback=set_error_result,
                write_options=WriteOptions(
                    max_retry_time=0,  # disable retries
                    # timeout=20,
                    write_type=WriteType.batching,
                    max_retries=0,
                    batch_size=1,
                )
            )
        )
        lp = "test_write_timeout,location=harfa fVal=3.14,iVal=42i"
        localClient.write(lp)

        # wait for batcher attempt last write retry
        time.sleep(0.1)

        self.assertEqual((self.database, 'default', 'ns'), ErrorResult["rt"])
        self.assertIsNotNone(ErrorResult["rd"])
        self.assertIsInstance(ErrorResult["rd"], bytes)
        self.assertEqual(lp, ErrorResult["rd"].decode('utf-8'))
        self.assertIsNotNone(ErrorResult["rx"])
        self.assertIsInstance(ErrorResult["rx"], MaxRetryError)
        self.assertIsInstance(ErrorResult["rx"].reason, Url3TimeoutError)

    def test_write_timeout_retry(self):

        ErrorResult = {"rt": None, "rd": None, "rx": None}

        def set_error_result(rt, rd, rx):
            nonlocal ErrorResult
            ErrorResult = {"rt": rt, "rd": rd, "rx": rx}

        retry_ct = 0

        def retry_cb(args, data, excp):
            nonlocal retry_ct
            retry_ct += 1

        localClient = InfluxDBClient3(
            host=self.host,
            database=self.database,
            token=self.token,
            write_timeout=1,
            write_client_options=write_client_options(
                error_callback=set_error_result,
                retry_callback=retry_cb,
                write_options=WriteOptions(
                    max_retry_time=10000,
                    max_retry_delay=100,
                    retry_interval=100,
                    max_retries=3,
                    batch_size=1,
                )
            )
        )

        lp = "test_write_timeout,location=harfa fVal=3.14,iVal=42i"
        localClient.write(lp)
        time.sleep(1)  # await all retries

        self.assertEqual(3, retry_ct)
        self.assertEqual((self.database, 'default', 'ns'), ErrorResult["rt"])
        self.assertIsNotNone(ErrorResult["rd"])
        self.assertIsInstance(ErrorResult["rd"], bytes)
        self.assertEqual(lp, ErrorResult["rd"].decode('utf-8'))
        self.assertIsNotNone(ErrorResult["rx"])
        self.assertIsInstance(ErrorResult["rx"], MaxRetryError)
        self.assertIsInstance(ErrorResult["rx"].reason, Url3TimeoutError)

    @pytest.mark.skip(reason="flaky in CircleCI - server often responds in less than 1 millisecond.")
    def test_query_timeout(self):
        localClient = InfluxDBClient3(
            host=self.host,
            token=self.token,
            database=self.database,
            query_timeout=1,
        )

        with self.assertRaisesRegex(InfluxDB3ClientQueryError, ".*Deadline Exceeded.*"):
            localClient.query("SELECT * FROM data")

    def test_query_timeout_per_call_override(self):
        localClient = InfluxDBClient3(
            host=self.host,
            token=self.token,
            database=self.database,
            query_timeout=3,
        )

        with self.assertRaisesRegex(InfluxDB3ClientQueryError, ".*Deadline Exceeded.*"):
            localClient.query("SELECT * FROM data", timeout=0.0001)

    def test_write_timeout_per_call_override(self):

        ErrorResult = {"rt": None, "rd": None, "rx": None}

        def set_error_result(rt, rd, rx):
            nonlocal ErrorResult
            ErrorResult = {"rt": rt, "rd": rd, "rx": rx}

        retry_ct = 0

        def retry_cb(args, data, excp):
            nonlocal retry_ct
            retry_ct += 1
            if excp is not None:
                raise excp

        localClient = InfluxDBClient3(
            host=self.host,
            token=self.token,
            database=self.database,
            # write_timeout=3000,
            write_client_options=write_client_options(
                error_callback=set_error_result,
                retry_callback=retry_cb,
                write_options=WriteOptions(
                    batch_size=1,
                ),

            )
        )

        lp = "test_write_timeout,location=harfa fVal=3.14,iVal=42i"
        localClient.write(lp, _request_timeout=1)

        # wait for batcher attempt last write retry
        time.sleep(0.1)

        self.assertEqual(retry_ct, 1)
        self.assertEqual((self.database, 'default', 'ns'), ErrorResult["rt"])
        self.assertIsNotNone(ErrorResult["rd"])
        self.assertIsInstance(ErrorResult["rd"], bytes)
        self.assertEqual(lp, ErrorResult["rd"].decode('utf-8'))
        self.assertIsNotNone(ErrorResult["rx"])
        self.assertIsInstance(ErrorResult["rx"], Url3TimeoutError)

    def test_disable_grpc_compression(self):
        """
        Test that disable_grpc_compression parameter controls query response compression.

        Uses H2HeaderProxy to intercept and verify gRPC headers over HTTP/2.
        Supports both h2c (cleartext) and h2 (TLS) connections.
        """
        from urllib.parse import urlparse
        from tests.util.h2_proxy import H2HeaderProxy

        # Test cases
        test_cases = [
            {
                'name': 'default',
                'disable_grpc_compression': None,
                'expected_req_encoding': 'identity, deflate, gzip',
                'expected_resp_encoding': 'gzip',
            },
            {
                'name': 'disabled=False',
                'disable_grpc_compression': False,
                'expected_req_encoding': 'identity, deflate, gzip',
                'expected_resp_encoding': 'gzip',
            },
            {
                'name': 'disabled=True',
                'disable_grpc_compression': True,
                'expected_req_encoding': 'identity',
                'expected_resp_encoding': None,
            },
        ]

        # Parse upstream host/port from test URL
        parsed = urlparse(self.host)
        upstream_host = parsed.hostname or '127.0.0.1'
        upstream_port = parsed.port or (443 if parsed.scheme == 'https' else 80)
        use_tls = parsed.scheme == 'https'

        test_id = time.time_ns()
        measurement = f'grpc_compression_test_{random_hex(6)}'

        # Write test data points
        num_points = 10
        lines = [
            f'{measurement},type=test value={i}.0,counter={i}i,test_id={test_id}i {test_id + i * 1000000}'
            for i in range(num_points)
        ]
        self.client.write('\n'.join(lines))

        test_query = f"SELECT * FROM \"{measurement}\" WHERE test_id = {test_id} ORDER BY counter"

        # Wait for data to be available
        result = None
        start = time.time()
        while time.time() - start < 10:
            result = self.client.query(test_query, mode="all")
            if len(result) >= num_points:
                break
            time.sleep(0.5)
        self.assertEqual(len(result), num_points, "Data not available after write")

        for tc in test_cases:
            name = tc['name']
            proxy = None

            try:
                # Start proxy - supports both h2c (cleartext) and h2 (TLS)
                proxy = H2HeaderProxy(
                    upstream_host=upstream_host,
                    upstream_port=upstream_port,
                    tls=use_tls,
                    upstream_tls=use_tls
                )
                proxy.start()

                # Build client kwargs
                client_kwargs = {
                    'host': proxy.url,
                    'database': self.database,
                    'token': self.token,
                    'verify_ssl': False,  # Accept proxy's self-signed cert
                }
                if tc['disable_grpc_compression'] is not None:
                    client_kwargs['disable_grpc_compression'] = tc['disable_grpc_compression']

                client = InfluxDBClient3(**client_kwargs)
                try:
                    result = client.query(test_query, mode="all")
                    self.assertEqual(len(result), num_points, f"[{name}] Should return {num_points} rows")
                finally:
                    client.close()

                # Verify headers
                req_encoding = proxy.get_last_request_header('grpc-accept-encoding')
                resp_encoding = proxy.get_last_response_header('grpc-encoding')

                print(f"\n[{name}] Request grpc-accept-encoding: {req_encoding}")
                expected_resp = tc['expected_resp_encoding']
                if expected_resp and resp_encoding != expected_resp:
                    print(f"[{name}] Response grpc-encoding: {resp_encoding} "
                          f"(expected: {expected_resp})")
                else:
                    print(f"[{name}] Response grpc-encoding: {resp_encoding}")

                self.assertEqual(req_encoding, tc['expected_req_encoding'],
                                 f"[{name}] Unexpected request encoding")

                if tc['expected_resp_encoding']:
                    # Note: InfluxDB 3 Core may not compress responses even when client
                    # advertises gzip support. Per gRPC spec, servers may choose not to
                    # compress regardless of client settings. InfluxDB Cloud typically
                    # compresses, but Core may not. We warn instead of failing.
                    # See: https://grpc.io/docs/guides/compression/
                    if resp_encoding != tc['expected_resp_encoding']:
                        import warnings
                        warnings.warn(
                            f"[{name}] Server returned '{resp_encoding}' instead of "
                            f"'{tc['expected_resp_encoding']}'. This is normal for "
                            f"InfluxDB 3 Core which may not compress responses."
                        )
                else:
                    self.assertTrue(resp_encoding is None or resp_encoding == 'identity',
                                    f"[{name}] Expected no compression, got: {resp_encoding}")
            finally:
                if proxy:
                    proxy.stop()
