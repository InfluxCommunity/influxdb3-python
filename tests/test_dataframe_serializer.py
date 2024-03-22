import unittest

from influxdb_client_3 import PointSettings
from influxdb_client_3.write_client.client.write.dataframe_serializer import DataframeSerializer
import pandas as pd


class TestDataFrameSerializer(unittest.TestCase):

    def test_nullable_types(self):
        df = pd.DataFrame({
            "bool_nulls": [True, None, False],
            "int_nulls": [None, 1, 2],
            "float_nulls": [1.0, 2.0, None],
            "str_nulls": ["a", "b", None],
        })
        df['bool_nulls_pd'] = df['bool_nulls'].astype(pd.BooleanDtype())
        df['int_nulls_pd'] = df['int_nulls'].astype(pd.Int64Dtype())
        df['float_nulls_pd'] = df['float_nulls'].astype(pd.Float64Dtype())
        df['str_nulls_pd'] = df['str_nulls'].astype(pd.StringDtype())

        df.index = pd.to_datetime(["2021-01-01", "2021-01-02", "2021-01-03"])

        ps = PointSettings()

        serializer = DataframeSerializer(df, ps, data_frame_measurement_name="test")

        lines = serializer.serialize()

        # make sure there are no `<NA>` values in the serialized lines
        # first line should not have "int"
        first_line = lines[0]
        self.assertNotIn('<NA>', first_line)
        self.assertNotIn('int_nulls', first_line)

        # the second line should not have "bool"
        second_line = lines[1]
        self.assertNotIn('<NA>', second_line)
        self.assertNotIn('bool_nulls', second_line)

        # the third line should not have "str" or "float"
        third_line = lines[2]
        self.assertNotIn('<NA>', third_line)
        self.assertNotIn('str_nulls', third_line)
        self.assertNotIn('float_nulls', third_line)
