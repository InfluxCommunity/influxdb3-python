# Change Log

## 1.11.0 [unreleased]

## 0.10.0 [2024-11-27]

### Bug Fixes

1. [#113](https://github.com/InfluxCommunity/influxdb3-python/pull/113): Fix import error of `PolarsDataframeSerializer` in batching mode

## 0.9.0 [2024-09-13]

### Features

1. [#108](https://github.com/InfluxCommunity/influxdb3-python/pull/108): Better expose access to response headers in `InfluxDBError`.  Example `handle_http_error` added.
2. [#112](https://github.com/InfluxCommunity/influxdb3-python/pull/112): Update batching examples, add integration tests of batching.

### Bug Fixes

1. [#107](https://github.com/InfluxCommunity/influxdb3-python/pull/107): Missing `py.typed` in distribution package
1. [#111](https://github.com/InfluxCommunity/influxdb3-python/pull/111): Reduce log level of disposal of batch processor to DEBUG

## 0.8.0 [2024-08-12]

### Features

1. [#101](https://github.com/InfluxCommunity/influxdb3-python/pull/101): Add support for InfluxDB Edge (OSS) authentication

### Bug Fixes

1. [#100](https://github.com/InfluxCommunity/influxdb3-python/pull/100): InfluxDB Edge (OSS) error handling
1. [#105](https://github.com/InfluxCommunity/influxdb3-python/pull/105): Importing Polars serialization module

## 0.7.0 [2024-07-11]

### Bug Fixes

1. [#95](https://github.com/InfluxCommunity/influxdb3-python/pull/95): `Polars` is optional dependency
1. [#99](https://github.com/InfluxCommunity/influxdb3-python/pull/99): Skip infinite values during serialization to line protocol

## 0.6.1 [2024-06-25]

### Bug Fixes

1. [#98](https://github.com/InfluxCommunity/influxdb3-python/pull/98): Missing declaration for `query` module

## 0.6.0 [2024-06-24]

### Features

1. [#89](https://github.com/InfluxCommunity/influxdb3-python/pull/89): Use `datetime.fromisoformat` over `dateutil.parse` in Python 3.11+
1. [#92](https://github.com/InfluxCommunity/influxdb3-python/pull/92): Update `user-agent` header value to `influxdb3-python/{VERSION}` and add it to queries as well. 

### Bug Fixes

1. [#86](https://github.com/InfluxCommunity/influxdb3-python/pull/86): Refactor to `timezone` specific `datetime` helpers to avoid use deprecated functions

## 0.5.0 [2024-05-17]

### Features

1. [#88](https://github.com/InfluxCommunity/influxdb3-python/pull/88): Add support for named query parameters:
   ```python
   from influxdb_client_3 import InfluxDBClient3

   with InfluxDBClient3(host="https://us-east-1-1.aws.cloud2.influxdata.com",
                        token="my-token",
                        database="my-database") as client:

        table = client.query("select * from cpu where host=$host", query_parameters={"host": "server01"})

        print(table.to_pandas())

    ```

### Bug Fixes

1. [#87](https://github.com/InfluxCommunity/influxdb3-python/pull/87): Fix examples to use `write_options` instead of the object name `WriteOptions`

### Others

1. [#84](https://github.com/InfluxCommunity/influxdb3-python/pull/84): Enable packaging type information - `py.typed`

## 0.4.0 [2024-04-17]

### Bugfix

1. [#77](https://github.com/InfluxCommunity/influxdb3-python/pull/77): Support using pandas nullable types

### Others

1. [#80](https://github.com/InfluxCommunity/influxdb3-python/pull/80): Integrate code style check into CI
