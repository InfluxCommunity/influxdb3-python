import unittest
import importlib.util

from influxdb_client_3 import PointSettings
from influxdb_client_3.write_client.client.write.polars_dataframe_serializer import polars_data_frame_to_list_of_points


@unittest.skipIf(importlib.util.find_spec("polars") is None, 'Polars package not installed')
class TestPolarsDataFrameSerializer(unittest.TestCase):

    def test_to_list_of_points(self):
        import polars as pl
        ps = PointSettings()
        df = pl.DataFrame(data={
            "name": ['iot-devices', 'iot-devices', 'iot-devices'],
            "building": ['5a', '5a', '5a'],
            "temperature": [72.3, 72.1, 72.2],
            "time": pl.Series(["2022-10-01T12:01:00Z", "2022-10-02T12:01:00Z", "2022-10-03T12:01:00Z"])
            .str.to_datetime(time_unit='ns')
        })
        actual = polars_data_frame_to_list_of_points(df, ps,
                                                     data_frame_measurement_name='iot-devices',
                                                     data_frame_tag_columns=['building'],
                                                     data_frame_timestamp_column='time')

        expected = [
            'iot-devices,building=5a name="iot-devices",temperature=72.3 1664625660000000000',
            'iot-devices,building=5a name="iot-devices",temperature=72.1 1664712060000000000',
            'iot-devices,building=5a name="iot-devices",temperature=72.2 1664798460000000000'
        ]
        self.assertEqual(expected, actual)
