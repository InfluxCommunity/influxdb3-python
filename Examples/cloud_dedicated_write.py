from config import Config
import influxdb_client_3 as InfluxDBClient3
from influxdb_client_3 import WriteOptions
import pandas as pd
import numpy as np

config = Config()

client = InfluxDBClient3.InfluxDBClient3(
    token=config.token,
    host=config.host,
    org=config.org,
    database=config.database,
    write_options=WriteOptions(
        batch_size=500,
        flush_interval=10_000,
        jitter_interval=2_000,
        retry_interval=5_000,
        max_retries=5,
        max_retry_delay=30_000,
        max_close_wait=300_000,
        exponential_base=2,
        write_type='batching'))


# Create a dataframe
df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})


# Create a range of datetime values
dates = pd.date_range(start='2024-09-08', end='2024-09-09', freq='5min')

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
