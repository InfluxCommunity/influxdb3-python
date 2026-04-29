#!/usr/bin/env python3
"""
fileimport.py - is a functional example that shows how to import data directly to Influxdb3
from other common database types.

The template databases used for this example can be found in `examples/write/source_data`. To create
fresh databases with current timestamps please run the helper file `Examples/write/source_data/updater.py`
before running fileimport.py.
"""
import logging
import os

import influxdb_client_3 as InfluxDBClient3
from influxdb_client_3 import write_client_options, WriteOptions, InfluxDBError


dir_path = os.path.dirname(os.path.realpath(__file__))

data_types = ["csv", "json", "feather", "orc", "parquet"]


class BatchingCallback(object):

    def __init__(self):
        self.write_count = 0

    def success(self, conf, data: bytes):
        self.write_count += 1
        print(f"Written batch: {conf}, data: {bytes(data)} bytes")

    def error(self, conf, data: bytes, exception: InfluxDBError):
        print(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def retry(self, conf, data: bytes, exception: InfluxDBError):
        print(f"Retryable error occurred for batch: {conf}, data: {bytes(data)} bytes, retry: {exception}")


def main(file_types=("csv",)) -> None:

    host = os.getenv('INFLUXDB_HOST') or 'http://localhost:8181'
    token = os.getenv('INFLUXDB_TOKEN') or 'my-token'
    database = os.getenv('INFLUXDB_DATABASE') or 'my-db'

    # allow detailed inspection
    if file_types is None:
        file_types = ["csv"]
    logging.basicConfig(level=logging.DEBUG)

    callback = BatchingCallback()

    write_options = WriteOptions(batch_size=100,
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

    """
       Note:
       debug: allows low-level inspection of communications and of context-manager termination
    """
    with InfluxDBClient3.InfluxDBClient3(
            token=token,
            host=host,
            database=database,
            write_client_options=wco,
            debug=True) as client:

        for _ftype in file_types:
            if _ftype not in data_types:
                logging.error(f"File type {_ftype} not supported.")
                continue

            logging.info(f"Writing from DB file of type: {_ftype}")
            source_file = f"{dir_path}/source_data/out_update.{_ftype}"
            if not (os.path.exists(source_file) and os.path.isfile(source_file)):
                logging.error(f"Source DB file {source_file} not found.")
                logging.error(" TIP!: Perhaps source_data/updater.py needs to be run.")
                continue
            # write data from file
            client.write_file(
                file=source_file,
                timestamp_column='time', tag_columns=["provider", "machineID"])

    print(f'DONE writing from {file_types} in {callback.write_count} batch(es)')


if __name__ == "__main__":
    main(("feather", "parquet", "orc", "csv", "json"))
