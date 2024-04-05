import influxdb_client_3 as InfluxDBClient3

client = InfluxDBClient3.InfluxDBClient3(
    token="",
    host="eu-central-1-1.aws.cloud2.influxdata.com",
    org="6a841c0c08328fb1",
    database="factory")


# Chunk mode provides a FlightReader object that can be used to read chunks of data.
reader = client.query(
    query="SELECT * FROM machine_data WHERE time > now() - 2h",
    language="influxql", mode="chunk")

try:
    while True:
        batch, buff = reader.read_chunk()
        print("batch:")
        print(batch.to_pandas())
except StopIteration:
    print("No more chunks to read")


# Pandas mode provides a Pandas DataFrame object.
df = client.query(
    query="SELECT * FROM machine_data WHERE time > now() - 2h",
    language="influxql", mode="pandas")

print("pandas:")
print(df)

# All mode provides an Arrow Table object.
table = client.query(
    query="SELECT * FROM machine_data WHERE time > now() - 2h",
    language="influxql", mode="all")

print("table:")
print(table)

# Print the schema of the table
table = client.query(
    query="SELECT * FROM machine_data WHERE time > now() - 2h",
    language="influxql", mode="schema")

print("schema:")
print(table)

# Convert this reader into a regular RecordBatchReader
reader = client.query(
    query="SELECT * FROM machine_data WHERE time > now() - 2h",
    language="influxql", mode="reader")

print("reader:")
for batch in reader:
    print(batch.to_pandas())
