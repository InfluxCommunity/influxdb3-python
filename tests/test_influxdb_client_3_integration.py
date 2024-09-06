import os
import time
import unittest

import pytest

from influxdb_client_3 import InfluxDBClient3, InfluxDBError


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

    def setUp(self):
        self.host = os.getenv('TESTING_INFLUXDB_URL')
        self.token = os.getenv('TESTING_INFLUXDB_TOKEN')
        self.database = os.getenv('TESTING_INFLUXDB_DATABASE')
        self.client = InfluxDBClient3(host=self.host, database=self.database, token=self.token)

    def tearDown(self):
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
            self.client.write(f"integration_test_python,type=used value=123.0,test_id=")
        self.assertIn("Could not parse entire line. Found trailing content:", err.exception.message)
        headers = err.exception.getheaders()
        try:
            self.assertIsNotNone(headers)
            self.assertRegex(headers['trace-id'], '[0-9a-f]{16}')
            self.assertEqual('false',headers['trace-sampled'])
            self.assertIsNotNone(headers['Strict-Transport-Security'])
            self.assertRegex(headers['X-Influxdb-Request-ID'], '[0-9a-f]+')
            self.assertIsNotNone(headers['X-Influxdb-Build'])
        except KeyError as ke:
            self.fail(f'Header {ke} not found')

