import influxdb_client_3 as InfluxDBClient3
import pandas as pd
import numpy as np

client = InfluxDBClient3.InfluxDBClient3(
    token="",
    host="b0c7cce5-8dbc-428e-98c6-7f996fb96467.a.influxdb.io",
    org="6a841c0c08328fb1",
    database="flight2")


table = client.query(
    query="SELECT * FROM flight WHERE time > now() - 4h",
    language="influxql")

print(table.to_pandas())
