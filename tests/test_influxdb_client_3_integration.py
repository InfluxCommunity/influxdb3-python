import os
import time
import unittest

import pytest

from influxdb_client_3 import InfluxDBClient3


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
        host = os.getenv('TESTING_INFLUXDB_URL')
        token = os.getenv('TESTING_INFLUXDB_TOKEN')
        database = os.getenv('TESTING_INFLUXDB_DATABASE')

        self.client = InfluxDBClient3(host=host, database=database, token=token)

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
