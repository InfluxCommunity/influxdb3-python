from config import Config
import influxdb_client_3 as InfluxDBClient3

config = Config()

client = InfluxDBClient3.InfluxDBClient3(
    token=config.token,
    host=config.host,
    org=config.org,
    database=config.database)

table = client.query(
    query="SELECT * FROM flight WHERE time > now() - 4h",
    language="influxql")

print(table.to_pandas())
