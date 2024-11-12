import importlib.util
import unittest
from unittest.mock import Mock

from influxdb_client_3 import PointSettings, InfluxDBClient3, write_client_options, WriteOptions
from influxdb_client_3.write_client import WriteService
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
            "time": pl.Series(["2024-08-01 00:00:00", "2024-08-01 01:00:00"]).str.to_datetime(time_unit='ns'),
            "temperature": [22.4, 21.8],
        })
        self.client._write_api._write_service = Mock(spec=WriteService)

        self.client.write(
            database="database",
            record=df,
            data_frame_measurement_name="measurement",
            data_frame_timestamp_column="time",
        )

        actual = self.client._write_api._write_service.post_write.call_args[1]['body']
        self.assertEqual(b'measurement temperature=22.4 1722470400000000000\n'
                         b'measurement temperature=21.8 1722474000000000000', actual)

    def test_write_polars_batching(self):
        import polars as pl
        df = pl.DataFrame({
            "time": pl.Series(["2024-08-01 00:00:00", "2024-08-01 01:00:00"]).str.to_datetime(time_unit='ns'),
            "temperature": [22.4, 21.8],
        })
        self.client = InfluxDBClient3(
            host="localhost",
            org="my_org",
            database="my_db",
            token="my_token", write_client_options=write_client_options(
                write_options=WriteOptions(batch_size=2)
            )
        )
        self.client._write_api._write_options = WriteOptions(batch_size=2)
        self.client._write_api._write_service = Mock(spec=WriteService)

        self.client.write(
            database="database",
            record=df,
            data_frame_measurement_name="measurement",
            data_frame_timestamp_column="time",
        )

        actual = self.client._write_api._write_service.post_write.call_args[1]['body']
        self.assertEqual(b'measurement temperature=22.4 1722470400000000000\n'
                         b'measurement temperature=21.8 1722474000000000000', actual)
