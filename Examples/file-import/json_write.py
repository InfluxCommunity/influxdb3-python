import influxdb_client_3 as InfluxDBClient3
import pandas as pd
import numpy as np
from influxdb_client_3 import write_client_options, WritePrecision, WriteOptions, InfluxDBError


class BatchingCallback(object):

    def success(self, conf, data: str):
        print(f"Written batch: {conf}, data: {data}")

    def error(self, conf, data: str, exception: InfluxDBError):
        print(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def retry(self, conf, data: str, exception: InfluxDBError):
        print(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")

callback = BatchingCallback()

write_options = WriteOptions(batch_size=500,
                                        flush_interval=10_000,
                                        jitter_interval=2_000,
                                        retry_interval=5_000,
                                        max_retries=5,
                                        max_retry_delay=30_000,
                                        exponential_base=2)

wco = write_client_options(success_callback=callback.success,
                          error_callback=callback.error,
                          retry_callback=callback.retry,
                          WriteOptions=write_options 
                        )

with  InfluxDBClient3.InfluxDBClient3(
    token="INSERT_TOKEN",
    host="eu-central-1-1.aws.cloud2.influxdata.com",
    org="6a841c0c08328fb1",
    database="python", _write_client_options=wco) as client:




    client.write_file(
        file='./out.json',
        timestamp_column='time', tag_columns=["provider", "machineID"], date_unit='ns' )

