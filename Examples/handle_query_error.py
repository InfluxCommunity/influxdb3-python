"""
Demonstrates handling error when querying InfluxDB.
"""
import logging
from config import Config
from influxdb_client_3.exceptions import InfluxDB3ClientQueryError

import influxdb_client_3 as InfluxDBClient3


def main() -> None:
    """
    Main function
    :return:
    """
    config = Config()
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    client = InfluxDBClient3.InfluxDBClient3(
        host=config.host,
        token=config.token,
        database=config.database
    )

    try:
        # Select from a bucket that doesn't exist
        client.query("Select a from cpu11")
    except InfluxDB3ClientQueryError as e:
        logging.log(logging.ERROR, e.message)


if __name__ == "__main__":
    main()
