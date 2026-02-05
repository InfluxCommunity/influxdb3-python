from pyarrow import flight

from config import Config
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


config = Config()
middleware = [ModifyHeaderClientMiddlewareFactory()]
client = InfluxDBClient3(
    host=config.host,
    token=config.token,
    database=config.database,
    flight_client_options=flight_client_options(middleware=middleware)
)

df = client.query(query="select * from cpu11 limit 10", mode="pandas")
print(len(df))
