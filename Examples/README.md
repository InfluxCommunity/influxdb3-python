## Infludb3 Python Examples

First time users will likely want to study the examples in the `./core` directory.  Users who work with __Jupyter__ notebooks may want to take a look at `basic-write-query.ipynb` in the `./jupyter` directory.

### Underlying principles

Influxdb3 uses two transports: one for writing data and another for querying.

### Writing data

Basic examples can be found in the `Examples/core` directory.

   * `basic_write.py` - shows the essentials of using the `Point` class and making simple writes. 
   * `basic_ssl.py` - shows how to handle SSL and TLS certificates. (TODO see point 7.2 below.)
   * `timeouts.py` - shows how to set and leverage timeout values. 

Richer examples can be found in the `Examples/write` directory.

   * `batching.py` - shows how to make use of the _batching_ API for writing long-running data sets.   
   * `fileimport.py` - shows how to import data to an Influx database directly from a number of other standard database formats.
       * To refresh the source data use `Examples/write/source_data/updater.py` 
   * `handle_http_error.py` - shows error handling on writes.
   * `pandas_write.py` - shows how to write pandas dataframes directly to an Influx database.
   * `writeoptions.py` - shows the core options API for writes. (TODO more specific?  See point 7.1 below)

### Querying data

Basic examples can be found in the `Examples/core` directory.

   * `basic_query.py` - shows the essentials of querying and Influxdb database.
   * `basic_ssl.py` - shows how to handle SSL and TLS certificates. (TODO see point 7.2 below.)
   * `timeouts.py` - shows how to set and leverage timeout values.

Richer examples can be found in the `Examples/query` directory.

   * `flight_options.py` - shows how to set options on the query transport.
   * `handle_query_error.py` - shows basic error handling.
   * `query_async.py` - shows basic usage of the asynchronous query API. 
   * `query_modes.py` - when making a query, different modes return data in different types of structures.  This example shows which modes return which structured formats.
   * `query_with_middleware.py` - when making a query it is possible to insert special headers into the HTTP layer using middleware.  This example shows how to do this.

### Advanced examples

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
       - what to do about step _write table to parquet file_? - not necessary. 
2. Keep `file-import` as single file, with source data updatable - see `updater.py` __DONE__
3. Keep one simple example of jupiter notebook. __DONE__
4. Decide what to do with `./community` examples.
   * `custom_url.py` 
       * Doesn't seem to do what is on the tin.
       * Seems more like a _down sampling_ example
       * Doesn't query Influxdb directly but down samples from remote CSV
       * TODO 
           * rename to _down sampling_ or similar
           * use initial query
           * move to `./advanced`
           * remove dependency on `githubusercontent`
   * `database_transfer.py` 
       * Simply copies data from one bucket to another
       * Useful base example
       * Current illustrative only - doesn't look functional without some undocumented setup
       * To work requires `dbfrom` and measurement `airSensors` to exist.
           * Note - are camel caps not sometimes problematic in Influx? (seem to recall encountering problem with them in the past)
       * TODO
           * move to `./advanced`
           * make functional
5. Root examples
   1. `basic_ssl_examle.py` to `./core` __DONE__
   2. `batching_example.py` to `./write` __DONE__
   3. `cloud_dedicated_query.py` - is it necessary to have specific _cloud_dedicated_ examples? Can this simply be documented in `README.md` or in code comments? (Removed __DONE__)
   4. `cloud_dedicated_write.py` - is it necessary to have specific _cloud_dedicated_ examples? Can this simply be documented in `README.md` or in code comments? (Removed __DONE__)
   5. `config.py` - universal configuration file.  Keep as is. 
   6. `example.csv` - where is this used?  It doesn't seem to be used in any example... ??? (Removed __DONE__)
   7. `flight_options_example.py` - to `./query` __DONE__
       - Note only option illustrated is tls certificate.  
       - TODO after move - either enrich/refactor or verify this isn't covered elsewhere
   8. `handle_http_error.py` - to `./write` __DONE__
   9. `handle_query_error.py` - to `./query` __DONE__
   10. `pandas_write.py` - to `./write` __DONE__
   11. `query_async.py` - to `./query`  __DONE__
   12. `query_type.py` - rename to `query_modes.py`. Move to `./query`  __DONE__
   13. `query_with_middleware.py` - to `./query` __DONE__
   14. `timeouts.py` - to `./core`  __DONE__ 
6. Standardization
   1. Some examples show leveraging internal **kwargs like `data_frame_measurement_name` or `data_frame_tag_columns`
       - Makes sense to expose these in _advanced_ examples. (e.g. pandas examples... )
       - Perhaps though should encourage the use of a simpler standard API that hides them
   2. Remove dependencies on remote `../githubusercontent/../*.csv` 
   2. Leverage `config.py` in all examples
   3. Prefer using Influxdb3 Core by default.  But also document possibility of using other products.
7. Enhancements
   1. `writeoptions.py` - does not show much in the way of setting options.
   2. `basic_ssl.py` - review. Seems to only show handling SSL handshake failures.
8. Verify all refactored examples are working
   *. __NOTE__ - If making an _illustrative_ example functional _out-of-the-box_ leads to too much distractive information being added, leave the example as _illustrative only_ and add a comment that it is for purposes of illustration.  However, make sure the illustrative example is still working in a concrete implementation. (e.g. `query_with_middleware.py`)
   *. TODO In comments, mark examples as either _illustrative_ or _functional_ 
