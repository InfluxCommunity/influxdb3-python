import influxdb_client_3 as InfluxDBClient3
from influxdb_client_3 import flight_client_options


with open("./cert.pem", 'rb') as f:
    cert = f.read()
print(cert)


client = InfluxDBClient3.InfluxDBClient3(
    token="",
    host="b0c7cce5-8dbc-428e-98c6-7f996fb96467.a.influxdb.io",
    org="6a841c0c08328fb1",
    database="flightdemo",
    flight_client_options=flight_client_options(
        tls_root_certs=cert))


table = client.query(
    query="SELECT * FROM flight WHERE time > now() - 4h",
    language="influxql")

print(table.to_pandas())
