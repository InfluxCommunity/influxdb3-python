import urllib.parse

import pyarrow as pa
import importlib.util

from influxdb_client_3.query.query_api import QueryApi as _QueryApi
from influxdb_client_3.read_file import UploadFile
from influxdb_client_3.write_client import InfluxDBClient as _InfluxDBClient, WriteOptions, Point
from influxdb_client_3.write_client.client.exceptions import InfluxDBError
from influxdb_client_3.write_client.client.write_api import WriteApi as _WriteApi, SYNCHRONOUS, ASYNCHRONOUS, \
    PointSettings
from influxdb_client_3.write_client.domain.write_precision import WritePrecision

polars = importlib.util.find_spec("polars") is not None


def write_client_options(**kwargs):
    """
    Function for providing additional arguments for the WriteApi client.

    :param kwargs: Additional arguments for the WriteApi client.
    :return: dict with the arguments.
    """
    return kwargs


def default_client_options(**kwargs):
    return kwargs


def flight_client_options(**kwargs):
    """
    Function for providing additional arguments for the FlightClient.

    :param kwargs: Additional arguments for the FlightClient.
    :return: dict with the arguments.
    """
    return kwargs


def file_parser_options(**kwargs):
    """
    Function for providing additional arguments for the file parser.

    :param kwargs: Additional arguments for the file parser.
    :return: dict with the arguments.
    """
    return kwargs


def _deep_merge(target, source):
    """
    Performs a deep merge of dictionaries or lists,
    recursively merging the contents, handling nested structures, and concatenation of lists.
    """
    if isinstance(target, dict) and isinstance(source, dict):
        for key, value in source.items():
            if key in target and isinstance(target[key], (dict, list)) and isinstance(value, (dict, list)):
                # If both target and source values are dictionaries or lists, merge them recursively
                target[key] = _deep_merge(target[key], value)
            else:
                # Otherwise, replace the target value with the source value
                target[key] = value
    elif isinstance(target, list) and isinstance(source, list):
        # If both target and source are lists, concatenate them
        target.extend(source)
    elif source is not None:
        # For other types, simply replace the target with the source
        target = source
    return target


def _merge_options(defaults, exclude_keys=None, custom=None):
    """
    Merge default option arguments with custom (user-provided) arguments,
    excluding specific keys defined in exclude_keys.
    """
    if custom is None or len(custom) == 0:
        return defaults

    if exclude_keys is None:
        exclude_keys = []

    return _deep_merge(defaults, {key: value for key, value in custom.items() if key not in exclude_keys})


class InfluxDBClient3:
    def __init__(
            self,
            host=None,
            org=None,
            database=None,
            token=None,
            write_client_options=None,
            flight_client_options=None,
            write_port_overwrite=None,
            query_port_overwrite=None,
            **kwargs):
        """
        Initialize an InfluxDB client.

        :param host: The hostname or IP address of the InfluxDB server.
        :type host: str
        :param org: The InfluxDB organization name for operations.
        :type org: str
        :param database: The database for InfluxDB operations.
        :type database: str
        :param token: The authentication token for accessing the InfluxDB server.
        :type token: str
        :param write_client_options: Function for providing additional arguments for the WriteApi client.
        :type write_client_options: callable
        :param flight_client_options: Function for providing additional arguments for the FlightClient.
        :type flight_client_options: callable
        :param kwargs: Additional arguments for the InfluxDB Client.
        """
        self._org = org if org is not None else "default"
        self._database = database
        self._token = token
        self._write_client_options = write_client_options if write_client_options is not None \
            else default_client_options(write_options=SYNCHRONOUS)

        # Parse the host input
        parsed_url = urllib.parse.urlparse(host)

        # Determine the protocol (scheme), hostname, and port
        scheme = parsed_url.scheme if parsed_url.scheme else "https"
        hostname = parsed_url.hostname if parsed_url.hostname else host
        port = parsed_url.port if parsed_url.port else 443

        # Construct the clients using the parsed values
        if write_port_overwrite is not None:
            port = write_port_overwrite

        self._client = _InfluxDBClient(
            url=f"{scheme}://{hostname}:{port}",
            token=self._token,
            org=self._org,
            **kwargs)

        self._write_api = _WriteApi(influxdb_client=self._client, **self._write_client_options)

        if query_port_overwrite is not None:
            port = query_port_overwrite
        if scheme == 'https':
            connection_string = f"grpc+tls://{hostname}:{port}"
        else:
            connection_string = f"grpc+tcp://{hostname}:{port}"
        self._query_api = _QueryApi(connection_string=connection_string, token=token,
                                    flight_client_options=flight_client_options)

    def write(self, record=None, database=None, **kwargs):
        """
        Write data to InfluxDB.

        :param record: The data point(s) to write.
        :type record: object or list of objects
        :param database: The database to write to. If not provided, uses the database provided during initialization.
        :type database: str
        :param kwargs: Additional arguments to pass to the write API.
        """
        if database is None:
            database = self._database

        try:
            self._write_api.write(bucket=database, record=record, **kwargs)
        except InfluxDBError as e:
            raise e

    def write_file(self, file, measurement_name=None, tag_columns=None, timestamp_column='time', database=None,
                   file_parser_options=None, **kwargs):
        """
        Write data from a file to InfluxDB.

        :param file: The file to write.
        :type file: str
        :param measurement_name: The name of the measurement.
        :type measurement_name: str
        :param tag_columns: Tag columns.
        :type tag_columns: list
        :param timestamp_column: Timestamp column name. Defaults to 'time'.
        :type timestamp_column: str
        :param database: The database to write to. If not provided, uses the database provided during initialization.
        :type database: str
        :param file_parser_options: Function for providing additional arguments for the file parser.
        :type file_parser_options: callable
        :param kwargs: Additional arguments to pass to the write API.
        """
        if database is None:
            database = self._database

        try:
            table = UploadFile(file, file_parser_options).load_file()
            df = table.to_pandas() if isinstance(table, pa.Table) else table
            self._process_dataframe(df, measurement_name, tag_columns or [], timestamp_column, database=database,
                                    **kwargs)
        except Exception as e:
            raise e

    def _process_dataframe(self, df, measurement_name, tag_columns, timestamp_column, database, **kwargs):
        # This function is factored out for clarity.
        # It processes a DataFrame before writing to InfluxDB.

        measurement_column = None
        if measurement_name is None:
            measurement_column = next((col for col in ['measurement', 'iox::measurement'] if col in df.columns), None)
            if measurement_column:
                for measurement in df[measurement_column].unique():
                    df_measurement = df[df[measurement_column] == measurement].drop(columns=[measurement_column])
                    self._write_api.write(bucket=self._database, record=df_measurement,
                                          data_frame_measurement_name=measurement,
                                          data_frame_tag_columns=tag_columns,
                                          data_frame_timestamp_column=timestamp_column)
            else:
                print("'measurement' column not found in the dataframe.")
        else:
            df = df.drop(columns=['measurement'], errors='ignore')
            self._write_api.write(bucket=database, record=df,
                                  data_frame_measurement_name=measurement_name,
                                  data_frame_tag_columns=tag_columns,
                                  data_frame_timestamp_column=timestamp_column, **kwargs)

    def query(self, query: str, language: str = "sql", mode: str = "all", database: str = None, **kwargs):
        """Query data from InfluxDB.

        If you want to use query parameters, you can pass them as kwargs:

        >>> client.query("select * from cpu where host=$host", query_parameters={"host": "server01"})

        :param query: The query to execute on the database.
        :param language: The query language to use. It should be one of "influxql" or "sql". Defaults to "sql".
        :param mode: The mode to use for the query. It should be one of "all", "pandas", "polars", "chunk",
                     "reader" or "schema". Defaults to "all".
        :param database: The database to query from. If not provided, uses the database provided during initialization.
        :param kwargs: Additional arguments to pass to the ``FlightCallOptions headers``. For example, it can be used to
                       set up per request headers.
        :keyword query_parameters: The query parameters to use in the query.
                                   It should be a ``dictionary`` of key-value pairs.
        :return: The query result in the specified mode.
        """
        if mode == "polars" and polars is False:
            raise ImportError("Polars is not installed. Please install it with `pip install polars`.")

        if database is None:
            database = self._database

        try:
            return self._query_api.query(query=query, language=language, mode=mode, database=database, **kwargs)
        except InfluxDBError as e:
            raise e

    def close(self):
        """Close the client and clean up resources."""
        self._write_api.close()
        self._query_api.close()
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


__all__ = [
    "InfluxDBClient3",
    "Point",
    "PointSettings",
    "SYNCHRONOUS",
    "ASYNCHRONOUS",
    "WritePrecision",
    "WriteOptions",
    "write_client_options",
    "flight_client_options",
    "file_parser_options"
]
