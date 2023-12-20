import os
import pyarrow.csv as csv
import pyarrow.feather as feather
import pyarrow.parquet as parquet

# Check if the OS is not Windows
if os.name != 'nt':
    import pyarrow.orc as orc


class UploadFile:
    """
    Class for uploading and reading different types of files.
    """
    def __init__(self, file, file_parser_options=None):
        """
        Initialize an UploadFile instance.

        :param file: The file to upload.
        :type file: str
        :param kwargs: Additional arguments for file loading functions.
        """

        self._file = file
        self._kwargs = file_parser_options if file_parser_options is not None else {}

    def load_file(self):
        """
        Load a file based on its extension.

        :return: The loaded file.
        :raises ValueError: If the file type is not supported.
        """
        if self._file.endswith(".feather"):
            return self.load_feather(self._file)
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

    def load_feather(self, file ):
        """
        Load a Feather file.

        :param file: The Feather file to load.
        :type file: str
        :return: The loaded Feather file.
        """
        return feather.read_table(file, **self._kwargs)

    def load_parquet(self, file):
        """
        Load a Parquet file.

        :param file: The Parquet file to load.
        :type file: str
        :return: The loaded Parquet file.
        """
        return parquet.read_table(file, **self._kwargs)

    def load_csv(self, file):
        """
        Load a CSV file.

        :param file: The CSV file to load.
        :type file: str
        :return: The loaded CSV file.
        """
        return csv.read_csv(file, **self._kwargs)

    def load_orc(self, file):
        """
        Load an ORC file.

        :param file: The ORC file to load.
        :type file: str
        :return: The loaded ORC file.
        :raises ValueError: If the OS is Windows.
        """
        if os.name == 'nt':
            raise ValueError("Unsupported file type for this OS")
        else:
            return orc.read_table(file, **self._kwargs)

    def load_json(self, file):
        """
        Load a JSON file.

        :param file: The JSON file to load.
        :type file: str
        :return: The loaded JSON file.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("Pandas is required for write_file(). Please install it using 'pip install pandas' or 'pip install influxdb3-python[pandas]'")
        
        return pd.read_json(file, **self._kwargs)