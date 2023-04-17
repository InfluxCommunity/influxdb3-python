# README.md for `influxdb_client_3` Python Module

## Introduction

`influxdb_client_3` is a Python module that provides a simple and convenient way to interact with InfluxDB 3.0. This module supports both writing data to InfluxDB and querying data using the FlightSQL client, which allows you to execute SQL queries on InfluxDB IOx.

## Dependencies

- `pyarrow`
- `flightsql-dbapi`
- `influxdb-client`

## Installation

You can install the dependencies using `pip`:

```bash
pip install pyarrow flightsql-dbapi influxdb-client
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
sql = "select * from measurement"
reader = client.query(sql_query=sql)
table = reader.read_all()
print(table.to_pandas().to_markdown())
```
