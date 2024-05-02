import datetime
import random

from bson import ObjectId

import influxdb_client_3 as InfluxDBClient3
from influxdb_client_3 import write_client_options, WritePrecision, WriteOptions, InfluxDBError


class BatchingCallback(object):

    def success(self, conf, data: str):
        print(f"Written batch: {conf}, data: {data}")

    def error(self, conf, data: str, exception: InfluxDBError):
        print(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def retry(self, conf, data: str, exception: InfluxDBError):
        print(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")


# Creating 5.000 gatewayId values as MongoDB ObjectIDs
gatewayIds = [ObjectId() for x in range(0, 100)]

# Setting decimal precision to 2
precision = 2

# Setting timestamp for first sensor reading
now = datetime.datetime.now()
now = now - datetime.timedelta(days=30)
teststart = datetime.datetime.now()

# InfluxDB connection details
token = ""
org = ""
database = ""
url = "eu-central-1-1.aws.cloud2.influxdata.com"

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
                           write_options=write_options
                           )
# Opening InfluxDB client with a batch size of 5k points or flush interval
# of 10k ms and gzip compression
with InfluxDBClient3.InfluxDBClient3(token=token,
                                     host=url,
                                     org=org,
                                     database=database, enable_gzip=True, write_client_options=wco) as _client:
    # Creating iterator for one hour worth of data (6 sensor readings per
    # minute)
    for i in range(0, 525600):
        # Adding 10 seconds to timestamp of previous sensor reading
        now = now + datetime.timedelta(seconds=10)
        # Iterating over gateways
        for gatewayId in gatewayIds:
            # Creating random test data for 12 fields to be stored in
            # timeseries database
            bcW = random.randrange(1501)
            bcWh = round(random.uniform(0, 4.17), precision)
            bdW = random.randrange(71)
            bdWh = round(random.uniform(0, 0.12), precision)
            cPvWh = round(random.uniform(0.51, 27.78), precision)
            cW = random.randrange(172, 10001)
            cWh = round(random.uniform(0.51, 27.78), precision)
            eWh = round(random.uniform(0, 41.67), precision)
            iWh = round(random.uniform(0, 16.67), precision)
            pW = random.randrange(209, 20001)
            pWh = round(random.uniform(0.58, 55.56), precision)
            scWh = round(random.uniform(0.58, 55.56), precision)
            # Creating point to be ingested into InfluxDB
            p = InfluxDBClient3.Point("stream").tag(
                "gatewayId",
                str(gatewayId)).field(
                "bcW",
                bcW).field(
                "bcWh",
                bcWh).field(
                "bdW",
                bdW).field(
                "bdWh",
                bdWh).field(
                "cPvWh",
                cPvWh).field(
                "cW",
                cW).field(
                "cWh",
                cWh).field(
                "eWh",
                eWh).field(
                "iWh",
                iWh).field(
                "pW",
                pW).field(
                "pWh",
                pWh).field(
                "scWh",
                scWh).time(
                now.strftime('%Y-%m-%dT%H:%M:%SZ'),
                WritePrecision.S)

            # Writing point (InfluxDB automatically batches writes into sets of
            # 5k points)
            _client.write(record=p)
