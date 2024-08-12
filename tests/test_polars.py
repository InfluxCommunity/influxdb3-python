import unittest
import importlib.util
from unittest.mock import Mock

from influxdb_client_3 import PointSettings, InfluxDBClient3
from influxdb_client_3.write_client.client.write_api import WriteApi

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


@unittest.skipIf(importlib.util.find_spec("polars") is None, 'Polars package not installed')
class TestWritePolars(unittest.TestCase):
    def setUp(self):
        self.client = InfluxDBClient3(
            host="localhost",
            org="my_org",
            database="my_db",
            token="my_token"
        )

    def test_write_polars(self):
        import polars as pl
        df = pl.DataFrame({
            "time": ["2024-08-01 00:00:00", "2024-08-01 01:00:00"],
            "temperature": [22.4, 21.8],
        })
        mock_write = Mock(spec=WriteApi)
        self.client._write_api.write = mock_write.write

        self.client.write(
            database="database",
            record=df,
            data_frame_measurement_name="measurement",
            data_frame_timestamp_column="time",
        )

        from polars.testing import assert_frame_equal
        assert_frame_equal(df, self.client._write_api.write.call_args[1]['record'])
