import logging
import influxdb_client_3 as InfluxDBClient3

from config import Config


def main() -> None:
    config = Config()
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

    client = InfluxDBClient3.InfluxDBClient3(
        host = config.host,
        token = config.token,
        org = config.org,
        database = config.database
    )

    # write with empty field results in HTTP 400 error
    # Other cases might be HTTP 503 or HTTP 429 too many requests
    lp = 'drone,location=harfa,id=A16E22 speed=18.7,alt=97.6,shutter='

    try:
        client.write(lp)
    except InfluxDBClient3.InfluxDBError as idberr:
        logging.log(logging.ERROR, f'WRITE ERROR: {idberr.response.status} ({idberr.message})')
        headersString = 'Response Headers:\n'
        headers = idberr.getheaders()
        for h in headers:
            headersString += f'   {h}: {headers[h]}\n'
        logging.log(logging.INFO, headersString)


if __name__ == "__main__":
    main()
