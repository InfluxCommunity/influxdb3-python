import datetime
import random
import time

from bson import ObjectId

import influxdb_client_3 as InfluxDBClient3
from influxdb_client_3 import write_client_options, WritePrecision, WriteOptions, InfluxDBError

from config import Config


class BatchingCallback(object):

    def __init__(self):
        self.write_status_msg = None
        self.write_count = 0
        self.retry_count = 0
        self.start = time.time_ns()

    def success(self, conf, data: str):
        print(f"Written batch: {conf}, data: {data}")
        self.write_count += 1
        self.write_status_msg = f"SUCCESS: {self.write_count} writes"

    def error(self, conf, data: str, exception: InfluxDBError):
        print(f"Cannot write batch: {conf}, data: {data} due: {exception}")
        self.write_status_msg = f"FAILURE - cause: {exception}"

    def retry(self, conf, data: str, exception: InfluxDBError):
        print(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")
        self.retry_count += 1

    def elapsed(self) -> int:
        return time.time_ns() - self.start


def main() -> None:
    conf = Config()

    # Creating 5.000 gatewayId values as MongoDB ObjectIDs
    gatewayIds = [ObjectId() for x in range(0, 100)]

    # Setting decimal precision to 2
    precision = 2

    # Setting timestamp for first sensor reading
    sample_window_days = 7
    now = datetime.datetime.now()
    now = now - datetime.timedelta(days=sample_window_days)
    target_sample_count = sample_window_days * 24 * 60 * 6

    callback = BatchingCallback()

    write_options = WriteOptions(batch_size=5_000,
                                 flush_interval=10_000,
                                 jitter_interval=2_000,
                                 retry_interval=5_000,
                                 max_retries=5,
                                 max_retry_delay=30_000,
                                 max_close_wait=600_000,
                                 exponential_base=2)

    wco = write_client_options(success_callback=callback.success,
                               error_callback=callback.error,
                               retry_callback=callback.retry,
                               write_options=write_options)

    # Opening InfluxDB client with a batch size of 5k points or flush interval
    # of 10k ms and gzip compression
    with InfluxDBClient3.InfluxDBClient3(token=conf.token,
                                         host=conf.host,
                                         org=conf.org,
                                         database=conf.database,
                                         enable_gzip=True,
                                         write_client_options=wco) as _client:
        # Creating iterator for one hour worth of data (6 sensor readings per
        # minute)
        print(f"Writing {target_sample_count} data points.")
        for i in range(0, target_sample_count):
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

    print(callback.write_status_msg)
    print(f"Write retries: {callback.retry_count}")
    print(f"Wrote {target_sample_count} data points.")
    print(f"Elapsed time ms: {int(callback.elapsed() / 1_000_000)}")


if __name__ == "__main__":
    main()
