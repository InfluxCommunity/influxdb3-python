#!/usr/bin/env python3
"""
pandas_write.py - is a functional example that demonstrates how to write data to Influxdb3
directly from a pandas DataFrame.
"""
import os
import pandas as pd
import numpy as np

import influxdb_client_3 as InfluxDBClient3


def main():

    host = os.getenv('INFLUXDB_HOST') or 'http://localhost:8181'
    token = os.getenv('INFLUXDB_TOKEN') or 'my-token'
    database = os.getenv('INFLUXDB_DATABASE') or 'my-db'

    with InfluxDBClient3.InfluxDBClient3(
            token=token,
            host=host,
            database=database) as client:

        # Create a dataframe
        df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})

        # Create a range of datetime values
        now = pd.Timestamp.now(tz="utc").floor(freq="min")
        start = now - pd.Timedelta(days=30)

        dates = pd.date_range(start=start, end=now, freq='5min')

        # Create a DataFrame with random data and datetime index
        df = pd.DataFrame(
            np.random.randn(
                len(dates),
                3),
            index=dates,
            columns=[
                'Column 1',
                'Column 2',
                'Column 3'])
        df['tagkey'] = 'Hello World'

        print(df)

        # Write the DataFrame to InfluxDB
        # Please note the keyword values used to declare
        # the measurement name and which DataFrame columns
        # will be written as tags.
        try:
            client.write(df, data_frame_measurement_name='pd_table',
                         data_frame_tag_columns=['tagkey'])
            print("Write Success!")
        except Exception as e:
            print(f"Write failure: {e}")


if __name__ == '__main__':
    main()
