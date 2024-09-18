import logging
import influxdb_client_3 as InfluxDBClient3
from influxdb_client_3 import write_client_options, WriteOptions, InfluxDBError


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


def main() -> None:

    # allow detailed inspection
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
       org: organization associated with account and database
       database: should have retention policy 'forever' to handle older sample data timestamps
       write_client_options: see above
       debug: allows low-level inspection of communications and context-manager termination
    """
    with (InfluxDBClient3.InfluxDBClient3(
            token="INSERT_TOKEN",
            host="https://us-east-1-1.aws.cloud2.influxdata.com/",
            org="INSERT_ORG",
            database="example_data_forever",
            write_client_options=wco,
            debug=True) as client):
        client.write_file(
            file='./out.csv',
            timestamp_column='time', tag_columns=["provider", "machineID"])

    print(f'DONE writing from csv in {callback.write_count} batch(es)')


if __name__ == "__main__":
    main()
