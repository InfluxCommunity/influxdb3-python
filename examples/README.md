## Infludb3 Python Examples

First time users will likely want to study the examples in the `./core` directory.  Users who work with __Jupyter__ notebooks may want to take a look at `basic-write-query.ipynb` in the `./jupyter` directory.

### Underlying principles

Influxdb3 uses two transports: one for writing data and another for querying.  For __writes__ a standard _HTTP REST_ style client is used internally.  __Queries__ on the other hand make use of an HTTP/2.0 and a _[gRPC](https://grpc.io/docs/what-is-grpc/introduction/)_ compliant client under _[Apache Arrow Flight](https://arrow.apache.org/cookbook/py/flight.html)_.

Most of the examples found here are functional and should be runnable against an Influxdb3 database, whether in the cloud or locally using for example Influxdb3 Core.  They have been revised and tested against the Influxdb3 Core product.

Some more advanced examples contain only illustrative code that can be reused in your license compliant applications.  Whether an example is intended to be simply _illustrative_ or _functional_ is noted in comments at the start of each example file.

__Configuration__

Functional examples make use of the three basic environment variables.

* `INFLUXDB_HOST` - host URL to connect to an Influxdb3 database.
* `INFLUXDB_DATABASE` - default database to be used with the examples.
* `INFLUXDB_TOKEN` - a token associated with read and write permissions to the default database and any additional databases that might be used with the examples.

These need to be set before running any example.

It is recommended to run examples using a python virtual environment.

For example...

```bash
$ python -m venv venv
$ source ./venv/bin/activate
```

Before running any functional examples, ensure that the Influxdb3-python project is installed.  From the `influxdb3-python` project root run...

```bash
$ pip install .
```

A few of the examples depend on libraries not included in Influxdb3 python.  The `examples/prep.py` script will install any missing example dependencies and set functional examples as executable.

```bash
$ python examples/prep.py
```

Functional examples can now be executed from the command line. 

e.g. 

```bash
$ examples/core/basic_write.py 
First point written to InfluxDB!
Write success: 3 points!
```

### Writing data

Basic examples can be found in the `Examples/core` directory.

   * `basic_write.py` - shows the essentials of using the `Point` class and making simple writes. 
   * `basic_ssl.py` - shows how to handle special SSL/TLS situations.
   * `timeouts.py` - shows how to set and leverage timeout values. 

Richer examples can be found in the `Examples/write` directory.

   * `batching.py` - shows how to make use of the _batching_ API for writing long-running data sets.   
   * `fileimport.py` - shows how to import data to an Influx database directly from a number of other standard database formats.
       * To refresh the source data used in the example, please run `Examples/write/source_data/updater.py` beforehand.
   * `handle_http_error.py` - shows error handling on writes.
   * `pandas_write.py` - shows how to write pandas dataframes directly to an Influx database.
   * `writeoptions.py` - shows the core options API for writes.

### Querying data

Basic examples can be found in the `Examples/core` directory.

   * `basic_query.py` - shows the essentials of querying and Influxdb database.
   * `basic_ssl.py` - shows how to handle special SSL/TLS situations.
   * `timeouts.py` - shows how to set and leverage timeout values.

Richer examples can be found in the `Examples/query` directory.

   * `flight_options.py` - shows how to set options on the query transport.
   * `handle_query_error.py` - shows basic error handling.
   * `query_async.py` - shows basic usage of the asynchronous query API. 
   * `query_modes.py` - when making a query, different modes return data in different types of structures.  This example shows which modes return which structured formats.
   * `query_with_middleware.py` - when making a query it is possible to insert special headers into the HTTP layer using middleware.  This example shows how to do this.

### Advanced examples

   * `database_transfer.py` - illustrates writing datapoints from one database to another.
   * `downsample.py` - shows how to read data from one measurement, reduce it to a smaller data set, and then write the new data set as a new measurement.
