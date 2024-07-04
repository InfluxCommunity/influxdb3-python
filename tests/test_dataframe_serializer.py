import unittest

from influxdb_client_3 import PointSettings
from influxdb_client_3.write_client.client.write.dataframe_serializer import DataframeSerializer, \
    data_frame_to_list_of_points
import pandas as pd
import numpy as np


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

    def test_null_and_inf_values(self):
        df = pd.DataFrame({
            "name": ['iot-devices', 'iot-devices', 'iot-devices'],
            "building": ['5a', '5a', '5a'],
            "temperature": pd.Series([72.3, -np.inf, np.inf]).astype(pd.Float64Dtype()),
            "time": pd.to_datetime(["2022-10-01T12:01:00Z", "2022-10-02T12:01:00Z", "2022-10-03T12:01:00Z"])
            .astype('datetime64[s, UTC]'),
        })
        ps = PointSettings()
        actual = data_frame_to_list_of_points(df, ps,
                                              data_frame_measurement_name='iot-devices',
                                              data_frame_tag_columns=['building'],
                                              data_frame_timestamp_column='time')

        expected = [
            'iot-devices,building=5a name="iot-devices",temperature=72.3 1664625660000000000',
            'iot-devices,building=5a name="iot-devices" 1664712060000000000',
            'iot-devices,building=5a name="iot-devices" 1664798460000000000'
        ]
        self.assertEqual(expected, actual)

    def test_to_list_of_points(self):
        df = pd.DataFrame({
            "name": ['iot-devices', 'iot-devices', 'iot-devices'],
            "building": ['5a', '5a', '5a'],
            "temperature": [72.3, 72.1, 72.2],
            "time": pd.to_datetime(["2022-10-01T12:01:00Z", "2022-10-02T12:01:00Z", "2022-10-03T12:01:00Z"])
            .astype('datetime64[s, UTC]'),
        })
        ps = PointSettings()
        actual = data_frame_to_list_of_points(df, ps,
                                              data_frame_measurement_name='iot-devices',
                                              data_frame_tag_columns=['building'],
                                              data_frame_timestamp_column='time')

        expected = [
            'iot-devices,building=5a name="iot-devices",temperature=72.3 1664625660000000000',
            'iot-devices,building=5a name="iot-devices",temperature=72.1 1664712060000000000',
            'iot-devices,building=5a name="iot-devices",temperature=72.2 1664798460000000000'
        ]
        self.assertEqual(expected, actual)
