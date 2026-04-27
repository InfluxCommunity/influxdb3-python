"""
query_with_middleware.py - is an illustrative example of how to add Arrow Flight middleware
when initializing a client.
"""
import os

from pyarrow import flight

from influxdb_client_3 import InfluxDBClient3, flight_client_options


# This middleware will add an additional attribute `some-attribute` to the header
class ModifyHeaderClientMiddleware(flight.ClientMiddleware):
    def sending_headers(self):
        return {
            "some-attribute": "some-value",
        }

    def received_headers(self, headers):
        pass


class ModifyHeaderClientMiddlewareFactory(flight.ClientMiddlewareFactory):
    def start_call(self, info):
        return ModifyHeaderClientMiddleware()


HOST = os.getenv('INFLUXDB_HOST') or 'http://localhost:8181'
TOKEN = os.getenv('INFLUXDB_TOKEN') or 'my-token'
DATABASE = os.getenv('INFLUXDB_DATABASE') or 'my-db'

middleware = [ModifyHeaderClientMiddlewareFactory()]
client = InfluxDBClient3(
    host=HOST,
    token=TOKEN,
    database=DATABASE,
    flight_client_options=flight_client_options(middleware=middleware)
)

df = client.query(query="select * from cpu11 limit 10", mode="pandas")
print(len(df))
