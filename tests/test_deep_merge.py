import unittest

import influxdb_client_3


class TestDeepMerge(unittest.TestCase):

    def test_deep_merge_dicts_with_no_overlap(self):
        target = {"a": 1, "b": 2}
        source = {"c": 3, "d": 4}
        result = influxdb_client_3._deep_merge(target, source)
        self.assertEqual(result, {"a": 1, "b": 2, "c": 3, "d": 4})

    def test_deep_merge_dicts_with_overlap(self):
        target = {"a": 1, "b": 2}
        source = {"b": 3, "c": 4}
        result = influxdb_client_3._deep_merge(target, source)
        self.assertEqual(result, {"a": 1, "b": 3, "c": 4})

    def test_deep_merge_nested_dicts(self):
        target = {"a": {"b": 1}}
        source = {"a": {"c": 2}}
        result = influxdb_client_3._deep_merge(target, source)
        self.assertEqual(result, {"a": {"b": 1, "c": 2}})

    def test_deep_merge_lists(self):
        target = [1, 2]
        source = [3, 4]
        result = influxdb_client_3._deep_merge(target, source)
        self.assertEqual(result, [1, 2, 3, 4])

    def test_deep_merge_non_overlapping_types(self):
        target = {"a": 1}
        source = [2, 3]
        result = influxdb_client_3._deep_merge(target, source)
        self.assertEqual(result, [2, 3])

    def test_deep_merge_none_to_flight(self):
        target = {
            "headers": [(b"authorization", "Bearer xyz".encode('utf-8'))],
            "timeout": 300
        }
        source = None
        result = influxdb_client_3._deep_merge(target, source)
        self.assertEqual(result, target)

    def test_deep_merge_empty_to_flight(self):
        target = {
            "headers": [(b"authorization", "Bearer xyz".encode('utf-8'))],
            "timeout": 300
        }
        source = {}
        result = influxdb_client_3._deep_merge(target, source)
        self.assertEqual(result, target)
