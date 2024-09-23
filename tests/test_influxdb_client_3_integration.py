import logging
import os
import random
import string
import time
import unittest

import pyarrow
import pytest

from influxdb_client_3 import InfluxDBClient3, InfluxDBError, write_client_options, WriteOptions


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
        self.assertEqual('unauthorized access', err.exception.message)  # Cloud

    def test_auth_error_auth_scheme(self):
        self.client = InfluxDBClient3(host=self.host, database=self.database, token=self.token, auth_scheme='Any')
        test_id = time.time_ns()
        with self.assertRaises(InfluxDBError) as err:
            self.client.write(f"integration_test_python,type=used value=123.0,test_id={test_id}i")
        self.assertEqual('unauthorized access', err.exception.message)  # Cloud

    def test_error_headers(self):
        self.client = InfluxDBClient3(host=self.host, database=self.database, token=self.token)
        with self.assertRaises(InfluxDBError) as err:
            self.client.write("integration_test_python,type=used value=123.0,test_id=")
        self.assertIn("Could not parse entire line. Found trailing content:", err.exception.message)
        headers = err.exception.getheaders()
        try:
            self.assertIsNotNone(headers)
            self.assertRegex(headers['trace-id'], '[0-9a-f]{16}')
            self.assertEqual('false', headers['trace-sampled'])
            self.assertIsNotNone(headers['Strict-Transport-Security'])
            self.assertRegex(headers['X-Influxdb-Request-ID'], '[0-9a-f]+')
            self.assertIsNotNone(headers['X-Influxdb-Build'])
        except KeyError as ke:
            self.fail(f'Header {ke} not found')

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
