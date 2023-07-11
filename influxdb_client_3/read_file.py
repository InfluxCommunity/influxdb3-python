from pyarrow import json as pa_json
import pyarrow.csv as csv
import pyarrow.feather as feather
import pyarrow.parquet as parquet
import os

# Check if the OS is not Windows
if os.name != 'nt':
    import pyarrow.orc as orc

import pandas as pd

class upload_file:
    def __init__(self, file, **kwargs):
        self._file = file
        self._kwargs = kwargs

    def load_file(self):
            if self._file.endswith(".feather"):
                return self.load_feather(self._file, **self._kwargs)
            elif self._file.endswith(".parquet"):
                return self.load_parquet(self._file)
            elif self._file.endswith(".csv"):
                return self.load_csv(self._file)
            elif self._file.endswith(".json"):
                return self.load_json(self._file)
            elif self._file.endswith(".orc"):

                return self.load_orc(self._file)
            else:
                raise ValueError("Unsupported file type")

    def load_feather(self, file):
        return feather.read_table(file, **self._kwargs)
        
    def load_parquet(self, file):
        return parquet.read_table(file, **self._kwargs)
        
    def load_csv(self, file):
        return csv.read_csv(file, **self._kwargs)
    
    def load_orc(self, file):
        if os.name == 'nt':
            raise ValueError("Unsupported file type for this OS")
        else:
            return orc.read_table(file, **self._kwargs)
    
    #TODO: Use pyarrow.json.read_json() instead of pandas.read_json()
    def load_json(self, file):
        return pd.read_json(file, **self._kwargs)
    