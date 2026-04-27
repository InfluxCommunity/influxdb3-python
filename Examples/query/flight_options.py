"""
flight_options.py - is an illustrative example of how to set low level options for the Arrow Flight client
and even for its internal gRPC client.

Note that the query transport of Influxdb3 is built on top of Arrow Flight, so `flight_client_options` in
a broad sense is synonymous with setting low level "Query API Options".

TODO review this one more time
"""
import os

from influxdb_client_3 import InfluxDBClient3, flight_client_options


with open("./cert.pem", 'rb') as f:
    cert = f.read()
print(cert)

HOST = os.getenv('INFLUXDB_HOST') or 'http://localhost:8181'
TOKEN = os.getenv('INFLUXDB_TOKEN') or 'my-token'
DATABASE = os.getenv('INFLUXDB_DATABASE') or 'my-db'

client = InfluxDBClient3(
    token=TOKEN,
    host=HOST,
    database=DATABASE,
    flight_client_options=flight_client_options(  # Options passed directly to the underlying Arrow flight client
        tls_root_certs=cert,  # Use a non-standard root certificate
        disable_server_verification=True,  # N.B. unsafe - for illustration here
        generic_options=[  # options to be passed to the gRPC client used by Arrow flight
            ("grpc.keepalive_time_ms", 300000),
            ("grpc.keepalive_timeout_ms", 20000),
        ]
    )
)

table = client.query(
    query="SELECT * FROM flight WHERE time > now() - 4h",
    language="influxql")

print(table.to_pandas())
