import unittest
from unittest.mock import Mock

import pandas as pd

from influxdb_client_3 import InfluxDBClient3
from influxdb_client_3.write_client.client.write_api import WriteApi


def assert_dataframe_equal(a, b, msg=None):
    from pandas.testing import assert_frame_equal
    assert_frame_equal(a, b)


class TestWriteFile(unittest.TestCase):

    def setUp(self):
        self.client = InfluxDBClient3(
            host="localhost",
            org="my_org",
            database="my_db",
            token="my_token"
        )
        self.addTypeEqualityFunc(pd.DataFrame, assert_dataframe_equal)

    def test_write_file_csv(self):

        mock_write = Mock(spec=WriteApi)
        self.client._write_api.write = mock_write.write

        self.client.write_file(file='tests/data/iot.csv', timestamp_column='time', measurement_name="iot-devices",
                               tag_columns=["building"], write_precision='s')

        expected_df = pd.DataFrame({
            "name": ['iot-devices', 'iot-devices', 'iot-devices'],
            "building": ['5a', '5a', '5a'],
            "temperature": [72.3, 72.1, 72.2],
            "time": pd.to_datetime(["2022-10-01T12:01:00Z", "2022-10-02T12:01:00Z", "2022-10-03T12:01:00Z"])
            .astype('datetime64[s, UTC]'),
        })
        expected = {
            'bucket': 'my_db',
            'record': expected_df,
            'data_frame_measurement_name': 'iot-devices',
            'data_frame_tag_columns': ['building'],
            'data_frame_timestamp_column': 'time',
            'write_precision': 's'
        }

        _, actual = mock_write.write.call_args
        assert mock_write.write.call_count == 1
        assert expected_df.equals(actual['record'])

        # Although dataframes are equal using custom equality function (see above assertion),
        # it does not work for nested items (self.assertEqual(expected, actual) fails).
        # So remove dataframes and compare the remaining call args.
        del expected['record']
        del actual['record']
        assert actual == expected

        # this would be better instead of the above, but does not work
        # mock_write.write.assert_called_once_with(bucket='my-db', record=expected_df,
        #                                          data_frame_measurement_name='iot-devices',
        #                                          data_frame_tag_columns=['building'],
        #                                          data_frame_timestamp_column='time',
        #                                          write_precision='s')
