import unittest

from influxdb_client_3.write_client.domain.write_precision import WritePrecision
from influxdb_client_3.write_client.domain.write_precision_converter import WritePrecisionConverter


class TestWritePrecisionConverter(unittest.TestCase):

    def test_to_v2_api_string_valid(self):
        self.assertEqual(WritePrecisionConverter.to_v2_api_string(WritePrecision.NS), "ns")
        self.assertEqual(WritePrecisionConverter.to_v2_api_string(WritePrecision.US), "us")
        self.assertEqual(WritePrecisionConverter.to_v2_api_string(WritePrecision.MS), "ms")
        self.assertEqual(WritePrecisionConverter.to_v2_api_string(WritePrecision.S), "s")

    def test_to_v2_api_string_unsupported(self):
        with self.assertRaises(ValueError) as err:
            WritePrecisionConverter.to_v2_api_string("invalid_precision")
        self.assertIn("Unsupported precision 'invalid_precision'", str(err.exception))

        with self.assertRaises(ValueError) as err:
            WritePrecisionConverter.to_v2_api_string(123)
        self.assertIn("Unsupported precision '123'", str(err.exception))

    def test_to_v3_api_string_valid(self):
        self.assertEqual(WritePrecisionConverter.to_v3_api_string(WritePrecision.NS), "nanosecond")
        self.assertEqual(WritePrecisionConverter.to_v3_api_string(WritePrecision.US), "microsecond")
        self.assertEqual(WritePrecisionConverter.to_v3_api_string(WritePrecision.MS), "millisecond")
        self.assertEqual(WritePrecisionConverter.to_v3_api_string(WritePrecision.S), "second")

    def test_to_v3_api_string_unsupported(self):
        with self.assertRaises(ValueError) as err:
            WritePrecisionConverter.to_v3_api_string("unsupported_value")
        self.assertIn("Unsupported precision 'unsupported_value'", str(err.exception))

        with self.assertRaises(ValueError) as err:
            WritePrecisionConverter.to_v3_api_string(42)
        self.assertIn("Unsupported precision '42'", str(err.exception))
