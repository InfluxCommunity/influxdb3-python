import influxdb_client_3 as InfluxDBClient3
import pandas as pd
import numpy as np

client = InfluxDBClient3.InfluxDBClient3(
    token="",
    host="eu-central-1-1.aws.cloud2.influxdata.com",
    org="6a841c0c08328fb1",
    database="test")


client.write_csv(
    './Examples/example.csv',
    measurement_name='table2',
    timestamp_column='Date')
