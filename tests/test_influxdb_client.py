import unittest
from unittest.mock import patch

import influxdb_client_3
from influxdb_client_3 import WritePrecision


class TestInfluxDBClient(unittest.TestCase):
    @patch.dict('os.environ', {'INFLUX_HOST': 'http://localhost:9999', 'INFLUX_TOKEN': 'test_token',
                               'INFLUX_DATABASE': 'test_db', 'INFLUX_ORG': 'test_org',
                               'INFLUX_PRECISION': WritePrecision.MS, 'INFLUX_TIMEOUT': '6000',
                               'INFLUX_VERIFY_SSL': 'False',
                               'INFLUX_CERT_FILE': 'path_to_cert', 'INFLUX_CERT_KEY_FILE': 'path_to_cert_key',
                               'INFLUX_CERT_KEY_PASSWORD': 'cert_key_password', 'INFLUX_CONNECTION_POOL_MAXSIZE': '200',
                               'INFLUX_PROFILERS': 'prof1,prof2,prof3', 'INFLUX_TAG_NAME': 'Tag1'})
    def test_from_env(self):
        base = influxdb_client_3._InfluxDBClient.from_env(enable_gzip=True)
        self.assertEqual(base.url, "http://localhost:9999")
        self.assertEqual(base.org, "test_org")
        self.assertEqual(base.default_tags['name'], "Tag1")
        self.assertEqual(base.profilers, ['prof1', 'prof2', 'prof3'])
        self.assertEqual(base.conf.enable_gzip, True)
        self.assertEqual(base.conf.timeout, 6000)
        self.assertEqual(base.conf.verify_ssl, False)
        self.assertEqual(base.conf.connection_pool_maxsize, 200)
        self.assertEqual(base.conf.cert_file, "path_to_cert")
        self.assertEqual(base.conf.cert_key_file, "path_to_cert_key")
        self.assertEqual(base.conf.cert_key_password, "cert_key_password")
        self.assertEqual(base.auth_header_name, "Authorization")
        self.assertEqual(base.auth_header_value, "Token test_token")


if __name__ == '__main__':
    unittest.main()
