## Infludb3 Python Examples

First time users will likely want to study the examples in the `./core` directory.  Users who work with __Jupyter__ notebooks may want to take a look at `basic-write-query.ipynb` in the `./jupyter` directory. 

### Writing data

Basic examples can be found in the `write` directory.

* `fileimport.py` shows how to import data to an Influx database directly from a number of other standard database formats.
TODO - others

## Refactoring Notes 
TODO - delete this section as examples take shape and before creating PR. 

1. Want to remove `pokemon-trainer` and refactor examples to `core`, `write` and `query`
2. Keep `file-import` as single file, with source data updatable - see `updater.py`
   1. Note `feather` type writes data using measurement `machine_data` and not `machine_data_feather`
   2. Note `orc` type writes data using measurement `machine_data` and not `machine_data_orc`
   3. Note `parquet` type writes data using measurement `machine_data` and not `machine_data_parquet`
3. Keep one simple example of jupiter notebook 