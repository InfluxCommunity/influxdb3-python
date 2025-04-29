from influxdb_client_3 import InfluxDBClient3

client = InfluxDBClient3(
    token="",
    host="eu-central-1-1.aws.cloud2.influxdata.com",
    database="pokemon-codex")

sql = '''SELECT * FROM caught WHERE trainer = 'ash' AND time >= now() - interval '1 hour' LIMIT 5'''
table = client.query(query=sql, language='sql', mode='all')
print(table)

influxql = '''SELECT * FROM caught WHERE trainer = 'ash' AND time  > now() - 1h LIMIT 5'''
table = client.query(query=influxql, language='influxql', mode='pandas')
print(table)
