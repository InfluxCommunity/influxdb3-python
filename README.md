<p align="center">
    <img src="python-logo.png" alt="Your Image" width="150px">
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
    <a href="https://app.slack.com/huddle/influxcommunity/">
        <img src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social" alt="Community Slack">
    </a>
</p>

# InfluxDB 3.0 Python Client
## Introduction

`influxdb_client_3` is a Python module that provides a simple and convenient way to interact with InfluxDB 3.0. This module supports both writing data to InfluxDB and querying data using the Flight client, which allows you to execute SQL and InfluxQL queries on InfluxDB 3.0.

## Dependencies

- `pyarrow`
- `influxdb-client`

## Installation

You can install the dependencies using `pip`:

```bash
pip install influxdb3-client
```

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

## Querying with SQL
```python
query = "select * from measurement"
reader = client.query(query=query, language="sql")
table = reader.read_all()
print(table.to_pandas().to_markdown())
```

## Querying with influxql
```python
query = "select * from measurement"
reader = client.query(query=query, language="influxql")
table = reader.read_all()
print(table.to_pandas().to_markdown())
```
