import time

import influxdb_client_3 as InfluxDBClient3
from influxdb_client_3 import write_client_options, WriteOptions, InfluxDBError


class BatchingCallback(object):

    def success(self, conf, data: str):
        print(f"Written batch: {conf}, data: {data}")

    def error(self, conf, data: str, exception: InfluxDBError):
        print(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def retry(self, conf, data: str, exception: InfluxDBError):
        print(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")


# InfluxDB connection details
token = ""
org = "6a841c0c08328fb1"
dbfrom = "a"
dbto = "b"
url = "eu-central-1-1.aws.cloud2.influxdata.com"
measurement = "airSensors"
taglist = []

callback = BatchingCallback()

write_options = WriteOptions(batch_size=5_000,
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
# Opening InfluxDB client with a batch size of 5k points or flush interval
# of 10k ms and gzip compression
with InfluxDBClient3.InfluxDBClient3(token=token,
                                     host=url,
                                     org=org,
                                     enable_gzip=True, write_client_options=wco) as _client:
    query = f"SHOW TAG KEYS FROM {measurement}"
    tags = _client.query(query=query, language="influxql", database=dbfrom)
    tags = tags.to_pydict()
    taglist = tags['tagKey']

    query = f"SELECT * FROM {measurement}"
    reader = _client.query(query=query, language="influxql", database=dbfrom, mode="chunk")
    try:
        while True:
            batch, buff = reader.read_chunk()
            print("batch:")
            pd = batch.to_pandas()
            pd = pd.set_index('time')
            print(pd)
            _client.write(database=dbto, record=pd, data_frame_measurement_name=measurement,
                          data_frame_tag_columns=taglist)
            time.sleep(2)
    except StopIteration:
        print("No more chunks to read")
