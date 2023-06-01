import influxdb_client_3 as InfluxDBClient3
import pandas as pd
import numpy as np

client = InfluxDBClient3.InfluxDBClient3(
    token="",
    host="eu-central-1-1.aws.cloud2.influxdata.com",
    org="",
    database="")


# Create a dataframe
df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})


# Create a range of datetime values
dates = pd.date_range(start='2023-03-01', end='2023-03-29', freq='5min')

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
client.write(df, data_frame_measurement_name='table',
             data_frame_tag_columns=['tagkey'])
