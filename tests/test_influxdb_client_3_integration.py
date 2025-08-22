import logging
import os
import pyarrow
import pytest
import random
import string
import time
import unittest

from urllib3.exceptions import MaxRetryError, ConnectTimeoutError

from influxdb_client_3 import InfluxDBClient3, write_client_options, WriteOptions, SYNCHRONOUS, flight_client_options, \
    WriteType
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
        self.client = InfluxDBClient3(host=self.host, database=self.database, token=self.token)

    def tearDown(self):
        self._caplog.clear()
        self._caplog.set_level(logging.ERROR)
        if self.client:
            self.client.close()

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

    # TODO set sync test, also investigate behavior with batcher and retry
    # TODO do these need to be run with integration - won't mock suffice?
    def test_write_timeout_sync(self):

        ErrorRecord = None
        def set_error_record(error):
            nonlocal ErrorRecord
            ErrorRecord = error

        with pytest.raises(ConnectTimeoutError) as e:
            localClient = InfluxDBClient3(
                host=self.host,
                database=self.database,
                token=self.token,
                write_client_options=flight_client_options(
                    error_callback=set_error_record,
                    write_options=WriteOptions(
                        max_retry_time=0,
                        timeout=20,
                        write_type=WriteType.synchronous
                    )
                )
            )

            localClient.write("test_write_timeout,location=harfa fVal=3.14,iVal=42i")


    @pytest.mark.skip(reason="placeholder - partially implemented")
    @asyncio_run
    async def test_write_timeout_async(self):
        # fco = flight_client_options(max_retries=10, timeout=30_000)
        # print(f"DEBUG fco: {fco}")
        # TODO ensure API can handle either callback or thrown exception
        # TODO asserts based on solution

        ErrorRecord = None
        def set_error_record(error):
            nonlocal ErrorRecord
            ErrorRecord = error


        localClient = InfluxDBClient3(
            host=self.host,
            database=self.database,
            token=self.token,
            write_client_options=flight_client_options(
                error_callback=set_error_record,
                write_options=WriteOptions(
                    max_retry_time=0,
                    timeout=20,
                    write_type=WriteType.asynchronous
                )
            )
        )

        print(f"DEBUG localClient._write_client_options: {localClient._write_client_options['write_options'].__dict__}")
        print(f"DEBUG localClient._client._base._Configuration {localClient._client.conf.timeout}")

        applyResult = localClient.write("test_write_timeout,location=harfa fVal=3.14,iVal=42i")
        print(f"DEBUG applyResult: {applyResult}")
        result = applyResult.get()
        print(f"DEBUG result: {result}")


    def test_write_timeout_batching(self):

        ErrorResult = {"rt": None, "rd": None, "rx": None}

        def set_error_result(rt, rd, rx):
            nonlocal ErrorResult
            ErrorResult = {"rt": rt, "rd": rd, "rx": rx}

        localClient = InfluxDBClient3(
            host=self.host,
            database=self.database,
            token=self.token,
            write_client_options=flight_client_options(
                error_callback=set_error_result,
                write_options=WriteOptions(
                    max_retry_time=0,
                    timeout=20,
                    write_type=WriteType.batching,
                    max_retries=1,
                    batch_size=1,
                )
            )
        )
        lp = "test_write_timeout,location=harfa fVal=3.14,iVal=42i"
        localClient.write(lp)

        # wait for batcher attempt last write retry
        time.sleep(0.1)

        assert ErrorResult["rt"] == (self.database, 'default', 'ns')
        assert ErrorResult["rd"] is not None
        assert isinstance(ErrorResult["rd"], bytes)
        assert ErrorResult["rd"].decode('utf-8') == lp
        assert ErrorResult["rx"] is not None
        assert isinstance(ErrorResult["rx"], MaxRetryError)
        mre = ErrorResult["rx"]
        assert isinstance(mre.reason, ConnectTimeoutError)

    @pytest.mark.skip("place holder")
    def test_write_timeout_retry(self):
        # TODO
        print("DEBUG test_write_timeout_retry")
