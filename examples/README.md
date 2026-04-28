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

## Refactoring Notes 
TODO - delete this section as examples take shape and before creating PR. 

1. Want to remove `pokemon-trainer` and refactor examples to `core`, `write` and `query`
   1. `basic-write-errorhandling.py` can be removed. __DONE__
      - actually shows error handling for query
      - duplicates examples `handle_http_error.py` and `handle_query_error.py`
   2. `basic-write-writeoptions.py` to `./write` refactored __DONE__
   3. `pandas-write.py` can be removed - duplicates `./pandas_write.py` __DONE__
   4. `write-batching.py` can be removed - duplicates `./batching_example.py` __DONE__
   5. `write-batching-flight-call-options.py` to be removed __DONE__
       - This is an odd example.  Sets write options then performs only a query.  Also _flight_ applies only to query API.
       - apparently depends on other examples
       - write and query options are illustrated in other examples
   6. `cookbook.ipynb` - most of this is migrated to `./jupyter/basic-write-query.ipynb` - can be removed __DONE__
       - what to do about step _write table to parquet file_? - not necessary __DONE__.
2. Keep `file-import` as single file, with source data updatable - see `updater.py` __DONE__
3. Keep one simple example of jupiter notebook. __DONE__
4. Decide what to do with `./community` examples.
   * `custom_url.py` __DONE__ 
       * Doesn't seem to do what is on the tin.
       * Seems more like a _down sampling_ example
       * ~~Doesn't query Influxdb directly but down samples from remote CSV~~
       * review/enhance __DONE__
           * rename to _down sampling_ or similar __DONE__
           * use initial query __DONE__
           * move to `./advanced` __DONE__
           * remove dependency on `githubusercontent` __DONE__
   * `database_transfer.py` 
       * Simply copies data from one bucket to another
       * Useful base example
       * Current illustrative only - doesn't look functional without some undocumented setup
       * To work requires `dbfrom` and measurement `airSensors` to exist.
       * review __DONE__
           * move to `./advanced` __DONE__
           * ~~make functional~~ kept as _illustrative_ - `advanced/downsample.py` is similar _and_ functional
5. Root examples
   1. `basic_ssl_examle.py` to `./core` __DONE__
   2. `batching_example.py` to `./write` __DONE__
      1. Issue - getting interpreter shutdown before example ends. TODO - fix
      2. Issue - runs very slow - TODO speed up. 
   3. `cloud_dedicated_query.py` - is it necessary to have specific _cloud_dedicated_ examples? Can this simply be documented in `README.md` or in code comments? (Removed __DONE__)
   4. `cloud_dedicated_write.py` - is it necessary to have specific _cloud_dedicated_ examples? Can this simply be documented in `README.md` or in code comments? (Removed __DONE__)
   5. `config.py` - universal configuration file.  Keep as is. 
   6. `example.csv` - where is this used?  It doesn't seem to be used in any example... ??? (Removed __DONE__)
   7. `flight_options_example.py` - to `./query` __DONE__
       - ~~Note only option illustrated is tls certificate~~.
       - after move - either enrich/refactor or verify this isn't covered elsewhere  __DONE__
   8. `handle_http_error.py` - to `./write` __DONE__
   9. `handle_query_error.py` - to `./query` __DONE__
   10. `pandas_write.py` - to `./write` __DONE__
   11. `query_async.py` - to `./query`  __DONE__
   12. `query_type.py` - rename to `query_modes.py`. Move to `./query`  __DONE__
   13. `query_with_middleware.py` - to `./query` __DONE__
   14. `timeouts.py` - to `./core`  __DONE__ 
6. Standardization
   1. Some examples show leveraging internal **kwargs like `data_frame_measurement_name` or `data_frame_tag_columns`  __DONE__
       - Makes sense to expose these in _advanced_ examples. (e.g. pandas examples... )
       - Perhaps though should encourage the use of a simpler standard API that hides them
   2. Remove dependencies on remote `../githubusercontent/../*.csv`  __DONE__
   2. ~~Leverage `config.py` in all examples~~  __DONE__ 
       1. Undo and use standard ENVARS instead __DONE__
   3. Prefer using Influxdb3 Core by default.  But also document possibility of using other products.
   4. Add shebangs to functional examples __DONE__
   5. Ensure timestamps are current and not fixed for example to 2023 __DONE__
7. Enhancements
   1. `writeoptions.py` - does not show much in the way of setting options. (Update and revision - __DONE__)
   2. `basic_ssl.py` - review. Seems to only show handling SSL handshake failures. (Reviewed and updated - __DONE__)
   3. `write_pandas.py` - has fixed dates from 2023, make dynamic with current.  __DONE__
   3. `flight_options.py` - review. This example is nearly three years old, and flight/query options has been greatly enhanced since then. __DONE__
8. Verify all refactored examples are working
   *. __NOTE__ - If making an _illustrative_ example functional _out-of-the-box_ leads to too much distractive information being added, leave the example as _illustrative only_ and add a comment that it is for purposes of illustration.  However, make sure the illustrative example is still working in a concrete implementation. (e.g. `query_with_middleware.py`)
   *. In comments, mark examples as either _illustrative_ or _functional_ __DONE__

#### Standardization summary (F - functional, I - illustrative)

| example                         | F/I | Head Comment | shebang | ~~Config()~~ | ENVARS | **kwargs (adv)       | github user link | Timestamps                   | Notes                                   |
|---------------------------------|-----|--------------|---------|--------------|--------|----------------------|------------------|------------------------------|-----------------------------------------|
| advanced/database_transfer.py   | I   | Yes          | None    | None         | Yes    | Yes (pd)             | None           | N.A. - copied without change | Ready                                   |
| advanced/downsample.py          | F   | Yes          | Yes     | None         | Yes    | Yes (pd)             | None           | Dynamic / Current            | Ready                                   |
| core/basic_write.py             | F   | Yes          | Yes     | None         | Yes    | None                 | None           | Dynamic / Now                | Ready                                   |
| core/basic_query.py             | F   | Yes          | Yes     | None         | Yes    | None                 | None           | Read only                    | Ready                                   |
| core/basic_ssl.py               | F   | Yes          | Yes     | None         | Yes    | None                 | None           | Dynamic / Now                | Ready                                   |
| core/timeouts.py                | F   | Yes          | Yes     | None         | Yes    | None                 | None           | Now (implicit)               | Ready                                   |
| jupyter/basic-write-query.ipynb | F   | N/A          | N/A     | N/A          | Yes    | None                 | None           | Dynamic                      | Ready                                   |
| query/flight_options.py         | I   | Yes          | None    | None         | Yes    | None                 | None           | Read only                    | Ready                                   |
| query/handle_query_error.py     | F   | Yes          | Yes     | None         | Yes    | None                 | None           | Read only                    | Ready                                   |
| query/query_async.py            | F   | Yes          | Yes     | None         | Yes    | None                 | None           | Dynamic                      | Ready                                   |
| query/query_modes.py            | F   | Yes          | Yes     | None         | Yes    | None                 | None           | Dynamic                      | Ready                                   |
| query/query_with_middleware.py  | I   | Yes          | None    | None         | Yes    | None                 | None           | Read only                    | Ready                                   |
| write/batching.py               | F   | Yes          | Yes     | None         | Yes    | None                 | None           | Dynamic                      | Review                                  |
| write/fileimport.py             | F   | Yes          | Yes     | None         | Yes    | Yes (`file_write()`) | None           | Dynamic                      | Ready - kwargs here are part of example |
| write/handle_http_error.py      | F   | Yes          | Yes     | None         | Yes    | None                 | None           | N.A.                         | Ready - shows write error               |
| write/pandas_write.py           | F   | Yes          | Yes     | None         | Yes    | Yes (pd)             | None           | Dynamic                      | Ready - kwargs here are part of example |
| write/writeoptions              | F   | Yes          | Yes     | None         | Yes    | None                 | None           | Dynamic / now                | Ready                                   |

TODO Review - run any example marked as "Review" in a fresh venv after a fresh project checkout and ensure it executes without issue.