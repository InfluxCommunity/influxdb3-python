#!/usr/bin/env python3
"""
`writeoptions.py` - is a functional example, except for certain illustrative callbacks,
that shows the basic principles of setting up configuration properties for the standard
HTTP write client.
"""
import datetime
import logging
import os

from influxdb_client_3 import (exceptions, InfluxDBClient3, Point,
                               WriteOptions, WritePrecision, WriteType, write_client_options)

logger = logging.getLogger("writeoptions")


# An illustrative callback - see below
def error_callback(conf, data: bytes, exception: exceptions.InfluxDBError):
    now = datetime.datetime.now()
    logger.warning(f"[{now}] an error occurred on latest write: {exception}")
    logger.warning(f"   conf: {conf}")
    logger.warning(f"   data: {data}")


# An illustrative callback - see below
def success_callback(conf, data: bytes):
    now = datetime.datetime.now()
    logger.info(f"[{now}] data written: {len(bytes(data))} bytes")
    logger.debug(f"   conf: {conf}")


wo = WriteOptions(
    write_type=WriteType.synchronous,  # Type of write api to use
    no_sync=False,  # Whether to wait for synchronizing writes with server acknowledgements
    timeout=30_000,  # Time in milliseconds to wait for a post write response
    write_precision=WritePrecision.MS,  # Timestamp precision used when writing data points
)
"""
The WriteOptions class encapsulates basic configuration properties.

Applicable properties will depend upon the value of the `write_type` property.  This can be...
   * WriteType.asynchronous
   * WriteType.synchronous
   * WriteType.batching (Constructor Default) - see the example `write/batching.py` for more details.
"""

wco = write_client_options(write_options=wo,  # The core WriteOptions object to use
                           success_callback=success_callback,  # N.B. currently only used in with batching type
                           error_callback=error_callback,  # N.B. currently only used with batching type
                           )
"""
The dictionary created by the call to `write_client_options()` can add other write client properties
such as callback functions.  Note that the `write_options` property is not always required,
in which case a default WriteOptions object is used internally.

The InfluxDBClient3 constructor will leverage this dictionary when configuring the standard HTTP based write client.
"""

measurement = 'wo_caught'


def main():

    host = os.getenv('INFLUXDB_HOST') or 'http://localhost:8181'
    token = os.getenv('INFLUXDB_TOKEN') or 'my-token'
    database = os.getenv('INFLUXDB_DATABASE') or 'my-db'

    with InfluxDBClient3(
         token=token,
         host=host,
         database=database,
         write_client_options=wco,  # write client options get passed to the client instance here.
         debug=True) as client:

        now = datetime.datetime.now(datetime.timezone.utc)

        data = Point(measurement).tag("trainer", "ash").tag("id", "0006").tag("num", "1") \
            .field("caught", "charizard") \
            .field("level", 10).field("attack", 30) \
            .field("defense", 40).field("hp", 200) \
            .field("speed", 10) \
            .field("type1", "fire").field("type2", "flying") \
            .time(now)

        try:
            client.write(data)
        except Exception as e:
            print(f"Error writing point: {e}")

        data = [Point(measurement)  # point 1
                .tag("trainer", "ash")
                .tag("id", "0006")
                .tag("num", "1")
                .field("caught", "charizard")
                .field("level", 10)
                .field("attack", 30)
                .field("defense", 40)
                .field("hp", 200)
                .field("speed", 10)
                .field("type1", "fire")
                .field("type2", "flying")
                .time(now),

                Point(measurement)  # point 2
                .tag("trainer", "ash")
                .tag("id", "0007")
                .tag("num", "2")
                .field("caught", "bulbasaur")
                .field("level", 12)
                .field("attack", 31)
                .field("defense", 31)
                .field("hp", 190)
                .field("speed", 11)
                .field("type1", "grass")
                .field("type2", "poison")
                .time(now),

                Point(measurement)  # point 3
                .tag("trainer", "ash")
                .tag("id", "0008")
                .tag("num", "3")
                .field("caught", "squirtle")
                .field("level", 13)
                .field("attack", 29)
                .field("defense", 40)
                .field("hp", 180)
                .field("speed", 13)
                .field("type1", "water")
                .field("type2", None)
                .time(now)
                ]

        try:
            client.write(data)
            print(f"Write success: {len(data)} points!")
        except Exception as e:
            print(f"Error writing point: {e}")


if __name__ == "__main__":
    main()
