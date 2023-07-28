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
    <a href="https://github.com/InfluxCommunity/influxdb3-python/actions/workflows/pylint.yml">
        <img src="https://github.com/InfluxCommunity/influxdb3-python/actions/workflows/pylint.yml/badge.svg" alt="Lint Code Base">
    </a>
        <a href="https://github.com/InfluxCommunity/influxdb3-python/actions/workflows/python-publish.yml">
        <img src="https://github.com/InfluxCommunity/influxdb3-python/actions/workflows/python-publish.yml/badge.svg" alt="Lint Code Base">
    </a>
    <a href="https://influxcommunity.slack.com">
        <img src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social" alt="Community Slack">
    </a>
</p>

# InfluxDB 3.0 Python Client
## Introduction

`influxdb_client_3` is a Python module that provides a simple and convenient way to interact with InfluxDB 3.0. This module supports both writing data to InfluxDB and querying data using the Flight client, which allows you to execute SQL and InfluxQL queries on InfluxDB 3.0.

## Dependencies

- `pyarrow` (automatically installed)
- `influxdb-client` (automatically installed)
- `pandas` (optional)
  

## Installation

You can install 'influxdb3-python' using `pip`:

```bash
pip install influxdb3-python
```

Note: This does not include Pandas support. If you would like to use key features such as `to_pandas()`  and `write_file()` you will need to install `pandas` separately.

*Note: Please make sure you are using 3.6 or above. For the best performance use 3.11+*

# Usage
One of the easiest ways to get started is to checkout the ["Pokemon Trainer Cookbook"](Examples/pokemon-trainer/cookbook.ipynb). This scenario takes you through the basics of both the client library and Pyarrow.

## Importing the Module
```python
from influxdb_client_3 import InfluxDBClient3, Point
```

## Initialization
If you are using InfluxDB Cloud, then you should note that:
1. You will need to supply your org id, this is not necessary for InfluxDB Dedicated.
2. Use a bucketname for the database argument.

```python
client = InfluxDBClient3(token="your-token",
                         host="your-host",
                         org="your-org",
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

    def success(self, conf, data: str):
        print(f"Written batch: {conf}, data: {data}")

    def error(self, conf, data: str, exception: InfluxDBError):
        print(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def retry(self, conf, data: str, exception: InfluxDBError):
        print(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")

callback = BatchingCallback()

write_options = WriteOptions(batch_size=500,
                                        flush_interval=10_000,
                                        jitter_interval=2_000,
                                        retry_interval=5_000,
                                        max_retries=5,
                                        max_retry_delay=30_000,
                                        exponential_base=2)

wco = write_client_options(success_callback=callback.success,
                          error_callback=callback.error,
                          retry_callback=callback.retry,
                          WriteOptions=write_options 
                        )

with  InfluxDBClient3.InfluxDBClient3(
    token="INSERT_TOKEN",
    host="eu-central-1-1.aws.cloud2.influxdata.com",
    org="6a841c0c08328fb1",
    database="python", write_client_options=wco) as client:


    client.write_file(
        file='./out.csv',
        timestamp_column='time', tag_columns=["provider", "machineID"])
    
    client.write_file(
        file='./out.json',
        timestamp_column='time', tag_columns=["provider", "machineID"], date_unit='ns' )
    

```

## Querying 

### Querying with SQL
```python
query = "select * from measurement"
reader = client.query(query=query, language="sql")
table = reader.read_all()
print(table.to_pandas().to_markdown())
```

### Querying with influxql
```python
query = "select * from measurement"
reader = client.query(query=query, language="influxql")
table = reader.read_all()
print(table.to_pandas().to_markdown())
```

## Windows Users
Currently, Windows users require an extra installation when querying via Flight natively. This is due to the fact gRPC cannot locate Windows root certificates. To work around this please follow these steps:
Install `certifi`
```bash
pip install certifi
```
Next include certifi within the flight client options:
```python
import influxdb_client_3 as InfluxDBClient3
import pandas as pd
import numpy as np
from influxdb_client_3 import flight_client_options
import certifi

fh = open(certifi.where(), "r")
cert = fh.read()
fh.close()


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
```
You may also include your own root certificate via this manor aswell. 
