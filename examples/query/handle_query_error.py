#!/usr/bin/env python3
"""
handle_query_error.py - Is a functional example that demonstrates handling error when querying InfluxDB.
"""
import logging
import os

from influxdb_client_3 import InfluxDBClient3
from influxdb_client_3.exceptions import InfluxDB3ClientQueryError


def main() -> None:
    """
    Main function
    :return:
    """
    host = os.getenv('INFLUXDB_HOST') or 'http://localhost:8181'
    token = os.getenv('INFLUXDB_TOKEN') or 'my-token'
    database = os.getenv('INFLUXDB_DATABASE') or 'my-db'

    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    client = InfluxDBClient3(
        host=host,
        token=token,
        database=database
    )

    try:
        # Select from a bucket that doesn't exist
        client.query("Select a from cpu11")
    except InfluxDB3ClientQueryError as e:
        logging.log(logging.ERROR, e.message)


if __name__ == "__main__":
    main()
