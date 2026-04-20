## Infludb3 Python Examples

First time users will likely want to study the examples in the `./core` directory.  Users who work with __Jupyter__ notebooks may want to take a look at `basic-write-query.ipynb` in the `./jupyter` directory.

### Underlying principles

Influxdb3 uses two transports: one for writing data and another for querying.

### Writing data

Basic examples can be found in the `write` directory.

* `fileimport.py` shows how to import data to an Influx database directly from a number of other standard database formats.
TODO - others

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
6. Leverage `config.py` in all examples 
7. Verify all refactored examples are working
