<!--home-start-->
<p align="center">
    <img src="https://github.com/InfluxCommunity/influxdb3-python/blob/main/python-logo.png?raw=true" alt="Your Image" width="150px">
</p>

<p align="center">
    <a href="https://pypi.org/project/influxdb3-python/">
        <img src="https://img.shields.io/pypi/v/influxdb3-python.svg" alt="PyPI version">
    </a>
    <a href="https://pypi.org/project/influxdb3-python/">
        <img src="https://img.shields.io/pypi/dm/influxdb3-python.svg" alt="PyPI downloads">
    </a>
    <a href="https://github.com/InfluxCommunity/influxdb3-python/actions/workflows/codeql-analysis.yml">
        <img src="https://github.com/InfluxCommunity/influxdb3-python/actions/workflows/codeql-analysis.yml/badge.svg?branch=main" alt="CodeQL analysis">
    </a>
    <a href="https://dl.circleci.com/status-badge/redirect/gh/InfluxCommunity/influxdb3-python/tree/main">
        <img src="https://dl.circleci.com/status-badge/img/gh/InfluxCommunity/influxdb3-python/tree/main.svg?style=svg" alt="CircleCI">
    </a>
    <a href="https://codecov.io/gh/InfluxCommunity/influxdb3-python">
        <img src="https://codecov.io/gh/InfluxCommunity/influxdb3-python/branch/main/graph/badge.svg" alt="Code Cov"/>
    </a>
    <a href="https://influxcommunity.slack.com">
        <img src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social" alt="Community Slack">
    </a>
</p>

# InfluxDB 3.0 Python Client
## Introduction

`influxdb_client_3` is a Python module that provides a simple and convenient way to interact with InfluxDB 3.0. This module supports both writing data to InfluxDB and querying data using the Flight client, which allows you to execute SQL and InfluxQL queries on InfluxDB 3.0.

We offer a ["Getting Started: InfluxDB 3.0 Python Client Library"](https://www.youtube.com/watch?v=tpdONTm1GC8) video that goes over how to use the library and goes over the examples.
## Dependencies

- `pyarrow` (automatically installed)
- `pandas` (optional)


## Installation

You can install 'influxdb3-python' using `pip`:

```bash
pip install influxdb3-python
```

Note: This does not include Pandas support. If you would like to use key features such as `to_pandas()`  and `write_file()` you will need to install `pandas` separately.

*Note: Please make sure you are using 3.6 or above. For the best performance use 3.11+*

# Usage
One of the easiest ways to get started is to checkout the ["Pokemon Trainer Cookbook"](https://github.com/InfluxCommunity/influxdb3-python/blob/main/Examples/pokemon-trainer/cookbook.ipynb). This scenario takes you through the basics of both the client library and Pyarrow.

## Importing the Module
```python
from influxdb_client_3 import InfluxDBClient3, Point
```

## Initialization
If you are using InfluxDB Cloud, then you should note that:
1. Use bucket name for `database` or `bucket` in function argument.

```python
client = InfluxDBClient3(token="your-token",
                         host="your-host",
                         database="your-database")
```

## Writing Data
You can write data using the Point class, or supplying line protocol.

### Using Points
```python
point = Point("measurement").tag("location", "london").field("temperature", 42)
client.write(point)
```
### Using Line Protocol
```python
point = "measurement fieldname=0"
client.write(point)
```

### Write from file
Users can import data from CSV, JSON, Feather, ORC, Parquet
```python
import influxdb_client_3 as InfluxDBClient3
import pandas as pd
import numpy as np
from influxdb_client_3 import write_client_options, WritePrecision, WriteOptions, InfluxDBError


class BatchingCallback(object):

    def __init__(self):
        self.write_count = 0

    def success(self, conf, data: str):
        self.write_count += 1
        print(f"Written batch: {conf}, data: {data}")

    def error(self, conf, data: str, exception: InfluxDBError):
        print(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def retry(self, conf, data: str, exception: InfluxDBError):
        print(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")

callback = BatchingCallback()

write_options = WriteOptions(batch_size=100,
                                        flush_interval=10_000,
                                        jitter_interval=2_000,
                                        retry_interval=5_000,
                                        max_retries=5,
                                        max_retry_delay=30_000,
                                        exponential_base=2)

wco = write_client_options(success_callback=callback.success,
                          error_callback=callback.error,
                          retry_callback=callback.retry,
                          write_options=write_options
                        )

with  InfluxDBClient3.InfluxDBClient3(
    token="INSERT_TOKEN",
    host="eu-central-1-1.aws.cloud2.influxdata.com",
    database="python", write_client_options=wco) as client:


    client.write_file(
        file='./out.csv',
        timestamp_column='time', tag_columns=["provider", "machineID"])

print(f'DONE writing from csv in {callback.write_count} batch(es)')

```

### Pandas DataFrame
```python
import pandas as pd

# Create a DataFrame with a timestamp column
df = pd.DataFrame({
    'time': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
    'trainer': ['Ash', 'Misty', 'Brock'],
    'pokemon_id': [25, 120, 74],
    'pokemon_name': ['Pikachu', 'Staryu', 'Geodude']
})

# Write the DataFrame - timestamp_column is required for consistency
client.write_dataframe(
    df,
    measurement='caught',
    timestamp_column='time',
    tags=['trainer', 'pokemon_id']
)
```

### Polars DataFrame
```python
import polars as pl

# Create a DataFrame with a timestamp column
df = pl.DataFrame({
    'time': ['2024-01-01T00:00:00Z', '2024-01-02T00:00:00Z'],
    'trainer': ['Ash', 'Misty'],
    'pokemon_id': [25, 120],
    'pokemon_name': ['Pikachu', 'Staryu']
})

# Write the DataFrame - same API works for both pandas and polars
client.write_dataframe(
    df,
    measurement='caught',
    timestamp_column='time',
    tags=['trainer', 'pokemon_id']
)
```

## Querying

### Querying with SQL
```python
query = "select * from measurement"
reader = client.query(query=query, language="sql")
table = reader.read_all()
print(table.to_pandas().to_markdown())
```

### Querying to DataFrame
```python
# Query directly to a pandas DataFrame (default)
df = client.query_dataframe("SELECT * FROM caught WHERE trainer = 'Ash'")

# Query to a polars DataFrame
df = client.query_dataframe("SELECT * FROM caught", frame_type="polars")
```

### Querying with influxql
```python
query = "select * from measurement"
reader = client.query(query=query, language="influxql")
table = reader.read_all()
print(table.to_pandas().to_markdown())
```

### gRPC compression
The Python client supports gRPC response compression.  
If the server chooses to compress query responses (e.g., with gzip), the client
will automatically decompress them — no extra configuration is required.

## Windows Users
Currently, Windows users require an extra installation when querying via Flight natively. This is due to the fact gRPC cannot locate Windows root certificates. To work around this please follow these steps:
Install `certifi`
```bash
pip install certifi
```
Next include certifi within the flight client options:

```python
import certifi

import influxdb_client_3 as InfluxDBClient3
from influxdb_client_3 import flight_client_options

fh = open(certifi.where(), "r")
cert = fh.read()
fh.close()

client = InfluxDBClient3.InfluxDBClient3(
    token="",
    host="b0c7cce5-8dbc-428e-98c6-7f996fb96467.a.influxdb.io",
    database="flightdemo",
    flight_client_options=flight_client_options(
        tls_root_certs=cert))

table = client.query(
    query="SELECT * FROM flight WHERE time > now() - 4h",
    language="influxql")

print(table.to_pandas())
```

You may include your own root certificate in this manner as well.

If connecting to InfluxDB fails with error `DNS resolution failed` when using domain name, example `www.mydomain.com`, then try to set environment variable `GRPC_DNS_RESOLVER=native` to see if it works.

# Contributing

Tests are run using `pytest`.

```bash
# Clone the repository
git clone https://github.com/InfluxCommunity/influxdb3-python
cd influxdb3-python

# Create a virtual environment and activate it
python3 -m venv .venv
source .venv/bin/activate

# Install the package and its dependencies
pip install -e .[pandas,polars,dataframe,test]

# Run the tests
python -m pytest .
```
<!--home-end-->
