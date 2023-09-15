from influxdb_client_3 import InfluxDBClient3
import pandas as pd
from influxdb_client_3.debug import TracingClientMiddleWareFactory



client = InfluxDBClient3(
    token="",
    host="eu-central-1-1.aws.cloud2.influxdata.com",
    org="6a841c0c08328fb1",
    database="pokemon-codex",
    flight_client_options={"middleware": (TracingClientMiddleWareFactory(),)})


sql = '''SELECT * FROM caught WHERE trainer = 'ash' AND time >= now() - interval '1 hour' LIMIT 5'''
table = client.query(query=sql, language='sql', mode='all')
print(table)


influxql = '''SELECT * FROM caught WHERE trainer = 'ash' AND time  > now() - 1h LIMIT 5'''
reader = client.query(query=influxql, language='influxql', mode='chunk')
try:
    while True:
        batch, buff = reader.read_chunk()
        print("batch:")
        print(buff)
except StopIteration:
    print("No more chunks to read")