#!/usr/bin/env python3
"""
basic_query.py - is a functional example that shows the simplest ways in which to query data from an influxdb3 database.

It should be run after running basic_write.py, which prepares the measurements queried here.

For more information on working with query modes see the `query/query_modes.py` example.
"""
import os
from influxdb_client_3 import InfluxDBClient3

HOST = os.getenv('INFLUXDB_HOST') or 'http://localhost:8181'
TOKEN = os.getenv('INFLUXDB_TOKEN') or 'my-token'
DATABASE = os.getenv('INFLUXDB_DATABASE') or 'my-db'

client = InfluxDBClient3(
    token=TOKEN,
    host=HOST,
    database=DATABASE,)

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
