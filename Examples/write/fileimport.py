import logging
import os

import influxdb_client_3 as InfluxDBClient3
from influxdb_client_3 import write_client_options, WriteOptions, InfluxDBError

from Examples.config import Config

data_types = ["csv", "json", "feather", "orc", "parquet"]


class BatchingCallback(object):

    def __init__(self):
        self.write_count = 0

    def success(self, conf, data: str):
        self.write_count += 1
        print(f"Written batch: {conf}, data: {data}")

    def error(self, conf, data: str, exception: InfluxDBError):
        print(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def retry(self, conf, data: str, exception: InfluxDBError):
        print(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")


def main(file_types=("csv",)) -> None:

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
       token: access token generated in cloud
       host: ATTN could be another AWS region or even another cloud provider
       database: should have retention policy 'forever' to handle older sample data timestamps
       write_client_options: see above
       debug: allows low-level inspection of communications and context-manager termination
    """
    config = Config()

    with InfluxDBClient3.InfluxDBClient3(
            token=config.token,
            host=config.host,
            database=config.database,
            write_client_options=wco,
            debug=True) as client:

        for type in file_types:
            if type not in data_types:
                logging.error(f"File type {type} not supported.")
                continue

            logging.info(f"Writing from file of type: {type}")
            source_file = f"./source_data/out_update.{type}"
            if not (os.path.exists(source_file) and os.path.isfile(source_file)):
                logging.error(f"Source file {source_file} not found.")
                logging.error(" TIP!: Perhaps source_data/updater.py needs to be run.")
                continue
            # write data from file
            client.write_file(
                file=source_file,
                timestamp_column='time', tag_columns=["provider", "machineID"])

    print(f'DONE writing from {file_types} in {callback.write_count} batch(es)')


if __name__ == "__main__":
    main(("feather", "parquet", "orc", "csv", "json"))
