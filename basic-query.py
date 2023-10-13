from influxdb_client_3 import InfluxDBClient3
import pandas as pd



client = InfluxDBClient3(
    token="CAFzHCRxTE2_pvg3wrRZvWak24AQ6avYIdTrki8pljufGbiMv2WgJvNx24mOb6yoPptvvVOydSlz-LbjfsyvGA==",
    host="eu-central-1-1.aws.cloud2.influxdata.com",
    org="6a841c0c08328fb1",
    database="pokemon-codex")


sql = '''SELECT * FROM caught WHERE trainer = 'ash' AND time >= now() - interval '1 hour' LIMIT 5'''
table = client.query(query=sql, language='sql', mode='all')
print(table)

influxql = '''SELECT * FROM caught WHERE trainer = 'ash' AND time  > now() - 1h LIMIT 5'''
table = client.query(query=influxql, language='influxql', mode='polars')
print(table)

from influxdb_client_3.write_client.client.write.dataframe_serializer import PolarsDataframeSerializer

serializer = PolarsDataframeSerializer(table, None)
# Generate the line-protocol entries for the DataFrame.
line_protocol_entries = serializer.serialize()

# Print the line-protocol entries.
for line_protocol_entry in line_protocol_entries:
    print(line_protocol_entry)
