#!/usr/bin/env python3
"""
basic_query.py - shows the simplest ways in which to query data from an influxdb3 database.

As a functional example it should be run after running basic_write.py, which prepares the measurements
queried here.

For more information on working with query modes see the `query/query_modes.py` example.
"""
from Examples.config import Config
from influxdb_client_3 import InfluxDBClient3

config = Config()

client = InfluxDBClient3(
    token=config.token,
    host=config.host,
    database=config.database,)

measurement = "basic_caught"

print("Quering with sql to Arrow Table")
sql = f'''SELECT * FROM {measurement} WHERE trainer = 'ash' AND time >= now() - interval '1 hour' LIMIT 5'''
table = client.query(query=sql, language='sql', mode='all')
print(table)

print("Querying with influxql to pandas DataFrame")
influxql = f'''SELECT * FROM {measurement} WHERE trainer = 'ash' AND time  > now() - 1h LIMIT 5'''
table = client.query(query=influxql, language='influxql', mode='pandas')
print(table)

client.close()
