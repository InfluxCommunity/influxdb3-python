#!/usr/bin/env python3
import sys
import time

from influxdb_client_3 import InfluxDBClient3, write_client_options

"""
This example shows how to set query and write timeouts.
They can be set directly using arguments (write_timeout, query_timeout) in the client constructor.
They can also be overridden in write and query calls.

To trigger timeout and deadline expired exceptions, reset the timeout values in the examples or
supply new values as command line parameters.

Be sure to update the host, token a database values below, before running this example.
"""

DEFAULT_WRITE_TIMEOUT = 30_000  # in milliseconds
DEFAULT_QUERY_TIMEOUT = 120_000  # in milliseconds
DEFAULT_HOST = 'http://localhost:8181'
DEFAULT_TOKEN = 'my-token'
DEFAULT_DATABASE = 'test-data'


def handle_write_error_cb(rd, rt, rx):
    print(f"Got a write error: {rd}, {rt}, {rx}")


def main(w_to: int, q_to: int) -> None:
    print(f"main {w_to}, {q_to}")
    lp_data = "timeout_example,location=terra fVal=3.14,iVal=42i"
    with InfluxDBClient3(
        host=DEFAULT_HOST,
        token=DEFAULT_TOKEN,
        database=DEFAULT_DATABASE,
        write_timeout=w_to,
        query_timeout=q_to,
        write_client_options=write_client_options(
            error_handler=handle_write_error_cb
        )
    ) as client:

        try:
            # write with write timeout set in client
            client.write(record=lp_data)
            print("First write OK.")
            time.sleep(1)

            # write overriding client internal write timeout
            # to force an exception try a _request_timeout value like 1 ms
            client.write(record=lp_data, _request_timeout=9_000)
            print("Second write OK.")
            time.sleep(1)
        except Exception as e:
            print(f"Got exception on write: {e}")

        sql = "SELECT * FROM timeout_example"

        try:
            # query using query timeout set in client
            result = client.query(sql)
            print("\nFirst query result\n", result)
            time.sleep(1)

            # query overriding client internal query timeout
            # To force an exception try a timeout value like 0.001 seconds
            result = client.query(sql, timeout=3.0)
            print("\nSecond query result\n", result)
        except Exception as e:
            print(f"Got exception on query: {e}")


# To force timeout errors supply the corresponding command line arguments
if __name__ == "__main__":
    w_ct = DEFAULT_WRITE_TIMEOUT
    q_ct = DEFAULT_QUERY_TIMEOUT
    for index, arg in enumerate(sys.argv):
        if index == 1:
            w_ct = int(arg)
        if index == 2:
            q_ct = int(arg)

    main(w_ct, q_ct)
