import unittest

import influxdb_client_3


class TestMergeOptions(unittest.TestCase):

    def test_merge_with_empty_custom(self):
        defaults = {"a": 1, "b": 2}
        result = influxdb_client_3._merge_options(defaults, custom={})
        self.assertEqual(result, defaults)

    def test_merge_with_none_custom(self):
        defaults = {"a": 1, "b": 2}
        result = influxdb_client_3._merge_options(defaults, custom=None)
        self.assertEqual(result, defaults)

    def test_merge_with_no_excluded_keys(self):
        defaults = {"a": 1, "b": 2}
        custom = {"b": 3, "c": 4}
        result = influxdb_client_3._merge_options(defaults, custom=custom)
        self.assertEqual(result, {"a": 1, "b": 3, "c": 4})

    def test_merge_with_excluded_keys(self):
        defaults = {"a": 1, "b": 2}
        custom = {"b": 3, "c": 4}
        result = influxdb_client_3._merge_options(defaults, exclude_keys=["b"], custom=custom)
        self.assertEqual(result, {"a": 1, "b": 2, "c": 4})
