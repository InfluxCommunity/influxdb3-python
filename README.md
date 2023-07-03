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

- `pyarrow`
- `influxdb-client`
  
*These are installed as part of the package*

## Installation

You can install 'influxdb3-python' using `pip`:

```bash
pip install influxdb3-python
```

*Note: Please make sure you are using 3.6 or above. For the best performance use 3.11+*

# Usage
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
