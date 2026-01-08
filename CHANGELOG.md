# Change Log

## 0.18.0 [unreleased]

## 0.17.0 [2026-01-08]

### Features

1. [#177](https://github.com/InfluxCommunity/influxdb3-python/pull/177): Add dedicated DataFrame methods for improved usability and type safety:
   - `write_dataframe()`: New method for writing pandas and polars DataFrames with explicit parameters (`measurement`, `timestamp_column`, `tags`, `timestamp_timezone`).
   - `query_dataframe()`: New method for querying data directly to a pandas or polars DataFrame via the `frame_type` parameter.
   - Updated README with clear examples for DataFrame operations.
1. [#179](https://github.com/InfluxCommunity/influxdb3-python/pull/179): Add option to disable gRPC response
   compression for Flight queries:
    - `disable_grpc_compression` parameter in `InfluxDBClient3` constructor
    - `INFLUX_DISABLE_GRPC_COMPRESSION` environment variable support in `from_env()`
1. [#180](https://github.com/InfluxCommunity/influxdb3-python/pull/180): Add `flush()` method to `InfluxDBClient3`:
   - Allows flushing the write buffer without closing the client when using batching mode.
   - Enables applications to ensure data is written before querying, while keeping the client open for further operations.

### Bug Fixes

1. [#177](https://github.com/InfluxCommunity/influxdb3-python/pull/177): Fix `TypeError` when writing DataFrames. Serializer-specific kwargs (e.g., `data_frame_measurement_name`) are now filtered before being passed to the HTTP layer.

### CI

1. [#164](https://github.com/InfluxCommunity/influxdb3-python/pull/164): Fix pipelines not downloading the correct python images.
1. [#167](https://github.com/InfluxCommunity/influxdb3-python/pull/167): 
    - Remove incorrect symbol `>>` for config.yml.
    - Added spacing for `<<` and `>>` just for consistency.
1. [#176](https://github.com/InfluxCommunity/influxdb3-python/pull/176): Use `ConstantFlightServerDelayed` for timeout tests.
1. [#183](https://github.com/InfluxCommunity/influxdb3-python/pull/183): Temporarily add annotation to support Python 3.8.

## 0.16.0 [2025-09-15]

### Features

1. [#158](https://github.com/InfluxCommunity/influxdb3-python/pull/158)  Improved parameters for setting timeouts
   - `InfluxDB3Client()` constructor now directly specifies
      - `write_timeout` - timeout in milliseconds to be used when writing data.
      - `query_timeout` - timeout in milliseconds to be used when querying data.
   - Timeouts can be specifically overridden in direct method calls.
      - `client.write()` now propagates the argument  `_request_timeout` as an `int` in milliseconds on the call stack 
         even in batching mode.
      - `client.query()` now propagates the argument `timeout` as a `float` in seconds on the call stack.

### CI

1. [#153](https://github.com/InfluxCommunity/influxdb3-python/pull/153) Add tests for arm64 CircleCI.

## 0.15.0 [2025-08-12]

### Features

1. [#146](https://github.com/InfluxCommunity/influxdb3-python/pull/146): Add function to get InfluxDB version.
2. [#149](https://github.com/InfluxCommunity/influxdb3-python/pull/149): Run integration tests against a locally started InfluxDB 3 Core server.

## 0.14.0 [2025-06-18]

### Features

1. [#141](https://github.com/InfluxCommunity/influxdb3-python/pull/141) Move "setuptools" package to build dependency.
2. [#142](https://github.com/InfluxCommunity/influxdb3-python/pull/142):  Support fast writes without waiting for WAL
   persistence:
   - New write option (`WriteOptions.no_sync`) added: `True` value means faster write but without the confirmation that
     the data was persisted. Default value: `False`.
   - **Supported by self-managed InfluxDB 3 Core and Enterprise servers only!**
   - Also configurable via environment variable (`INFLUX_WRITE_NO_SYNC`).
   - Long precision string values added from v3 HTTP API: `"nanosecond"`, `"microsecond"`, `"millisecond"`,
     `"second"` (     in addition to the existing `"ns"`, `"us"`, `"ms"`, `"s"`).
3. [#145](https://github.com/InfluxCommunity/influxdb3-python/pull/145): Improve the document wording for README.md

## 0.13.0 [2025-05-20]

### Features

1. [#130](https://github.com/InfluxCommunity/influxdb3-python/pull/130): Remove org parameters from the example code because It is not mandatory in Influxdb3
2. [#139](https://github.com/InfluxCommunity/influxdb3-python/pull/139): Supports environment variables with the same name as other clients
3. [#140](https://github.com/InfluxCommunity/influxdb3-python/pull/140): Query api will throw `InfluxdbClientQueryError` when receiving `ArrowException` from gRPC servers

## 0.12.0 [2025-03-26] 

### Features

1. [#123](https://github.com/InfluxCommunity/influxdb3-python/pull/123): Introduces `query_async()` method. From this release the client now has a `query_async()` method that takes advantage of asyncio's event loop to run query calls in their own executor.

For example:
```python
    table = await client.query_async(query)
```

### Bug Fixes

1. [#121](https://github.com/InfluxCommunity/influxdb3-python/pull/121): Fix use of arguments `verify_ssl` and `ssl_ca_cert` in `QueryApi`.

## 0.11.0 [2025-02-27]

### Bug Fixes

1. [#119](https://github.com/InfluxCommunity/influxdb3-python/pull/119): Fix use of `proxy` argument in client and query_api to use in channel solution for GRPC proxy.

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
