"""
Demonstrates handling response error headers on error.
"""
import logging
from config import Config

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
        org=config.org,
        database=config.database
    )

    # write with empty field results in HTTP 400 error
    # Other cases might be HTTP 503 or HTTP 429 too many requests
    lp = 'drone,location=harfa,id=A16E22 speed=18.7,alt=97.6,shutter='

    try:
        client.write(lp)
    except InfluxDBClient3.InfluxDBError as idberr:
        logging.log(logging.ERROR, 'WRITE ERROR: %s (%s)',
                    idberr.response.status,
                    idberr.message)
        headers_string = 'Response Headers:\n'
        headers = idberr.getheaders()
        for h in headers:
            headers_string += f'   {h}: {headers[h]}\n'
        logging.log(logging.INFO, headers_string)


if __name__ == "__main__":
    main()
