#!/usr/bin/env python3
"""
query_modes.py - is a functional example that shows how to use different modes when executing queries.
"""
import pytz
import numpy as np

from datetime import datetime, timedelta

from influxdb_client_3 import InfluxDBClient3, Point
from Examples.config import Config


def prep_data(client: InfluxDBClient3, measurement: str):
    now = datetime.now(tz=pytz.utc)
    current = now - timedelta(minutes=5)

    points = []

    while current < now:
        points.append(Point(measurement)
                      .tag('model', np.random.choice(['R2D2', 'C3PO', 'ROBBIE', 'HAL']))
                      .field('mV', np.random.uniform(low=0, high=1000))
                      .field('mA', np.random.uniform(low=0, high=1000))
                      .field('dBm', np.random.uniform(low=-10, high=10))
                      .time(current))
        current += timedelta(seconds=10)

    client.write(points)


def query_chunk(client: InfluxDBClient3, influxql_query: str):
    # Chunk mode provides a FlightReader object that can be used to read chunks of data.
    reader = client.query(
        query=influxql_query,
        language="influxql", mode="chunk")

    try:
        while True:
            batch, buff = reader.read_chunk()
            print("batch:")
            print(batch.to_pandas())
    except StopIteration:
        print("No more chunks to read")


def query_pandas(client: InfluxDBClient3, influxql_query: str):
    # Pandas mode provides a Pandas DataFrame object.
    df = client.query(
        query=influxql_query,
        language="influxql", mode="pandas")

    print("pandas:")
    print(df)


def query_all(client: InfluxDBClient3, influxql_query: str):
    # All mode provides an Arrow Table object.
    table = client.query(
        query=influxql_query,
        language="influxql", mode="all")

    print("table:")
    print(table)


def query_schema(client: InfluxDBClient3, influxql_query: str):
    # Print the schema of the table
    table = client.query(
        query=influxql_query,
        language="influxql", mode="schema")

    print("schema:")
    print(table)


def query_reader(client: InfluxDBClient3, influxqul_query: str):
    # Convert this reader into a regular RecordBatchReader
    reader = client.query(
        query=influxqul_query,
        language="influxql", mode="reader")

    print("reader:")
    for batch in reader:
        print(batch.to_pandas())


def main(config: Config):
    measurement = "machine_data"
    influxql = f"SELECT * FROM {measurement} WHERE time > now() - 5m"
    with InfluxDBClient3(
        host=config.host,
        database=config.database,
        token=config.token
    ) as client:
        prep_data(client, measurement)
        print("\n=== Querying Chunks ===\n")
        query_chunk(client, influxql)
        print("\n=== Querying Pandas ===\n")
        query_pandas(client, influxql)
        print("\n=== Querying All ===\n")
        query_all(client, influxql)
        print("\n=== Querying Schema ===\n")
        query_schema(client, influxql)
        print("\n=== Querying Reader ===\n")
        query_reader(client, influxql)


if __name__ == "__main__":
    main(Config())
