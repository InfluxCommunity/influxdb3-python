import json
import urllib.parse
import pyarrow as pa
from influxdb_client import InfluxDBClient as _InfluxDBClient, WriteOptions, Point
from influxdb_client.client.write_api import WriteApi as _WriteApi, SYNCHRONOUS, ASYNCHRONOUS, PointSettings
from influxdb_client.domain.write_precision import WritePrecision
from influxdb_client.client.exceptions import InfluxDBError
from pyarrow.flight import FlightClient, Ticket, FlightCallOptions
from influxdb_client_3.read_file import upload_file


def write_client_options(**kwargs):
    return kwargs

def default_client_options(**kwargs):
    return kwargs

def flight_client_options(**kwargs):
    return kwargs  # You can replace this with a specific data structure if needed

class InfluxDBClient3:
    def __init__(
            self,
            host=None,
            org=None,
            database=None,
            token=None,
            write_client_options=None,
            flight_client_options=None,
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
        :param write_client_options: Options for the WriteAPI.
        :type write_client_options: dict
        :param flight_client_options: Options for the FlightClient.
        :type flight_client_options: dict
        :param kwargs: Additional arguments for the InfluxDB Client.
        """
        self._org = org
        self._database = database
        self._write_client_options = write_client_options if write_client_options is not None else default_client_options(write_options=SYNCHRONOUS)

        # Extracting the hostname from URL if provided
        parsed_url = urllib.parse.urlparse(host)
        host = parsed_url.hostname or host

        self._client = _InfluxDBClient(
            url=f"https://{host}",
            token=token,
            org=self._org,
            **kwargs)
        
        self._write_api = _WriteApi(influxdb_client=self._client, **self._write_client_options)
        self._flight_client_options = flight_client_options or {}
        self._flight_client = FlightClient(f"grpc+tls://{host}:443", **self._flight_client_options)
        
        # Create an authorization header
        self._options = FlightCallOptions(headers=[(b"authorization", f"Bearer {token}".encode('utf-8'))])

    def write(self, record=None, **kwargs):
        """
        Write data to InfluxDB.

        :param record: The data point(s) to write.
        :type record: Point or list of Point objects
        :param kwargs: Additional arguments to pass to the write API.
        """
        try:
            self._write_api.write(bucket=self._database, record=record, **kwargs)
        except InfluxDBError as e:
            raise e
          

    def write_file(self, file, measurement_name=None, tag_columns=None, timestamp_column='time', **kwargs):
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
        :param kwargs: Additional arguments to pass to the write API.
        """
        try:
            table = upload_file(file).load_file()
            df = table.to_pandas() if isinstance(table, pa.Table) else table
            self._process_dataframe(df, measurement_name, tag_columns or [], timestamp_column)
        except Exception as e:
            raise e
            

    def _process_dataframe(self, df, measurement_name, tag_columns, timestamp_column):
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
            self._write_api.write(bucket=self._database, record=df,
                                  data_frame_measurement_name=measurement_name,
                                  data_frame_tag_columns=tag_columns,
                                  data_frame_timestamp_column=timestamp_column)

    def query(self, query, language="sql", mode="all"):
        """
        Query data from InfluxDB.

        :param query: The query string.
        :type query: str
        :param language: The query language (default is "sql").
        :type language: str
        :param mode: The mode of fetching data (all, pandas, chunk, reader, schema).
        :type mode: str
        :return: The queried data.
        """
        try:
            ticket_data = {"database": self._database, "sql_query": query, "query_type": language}
            ticket = Ticket(json.dumps(ticket_data).encode('utf-8'))
            flight_reader = self._flight_client.do_get(ticket, self._options)

            mode_func = {
                "all": flight_reader.read_all,
                "pandas": flight_reader.read_pandas,
                "chunk": lambda: flight_reader,
                "reader": flight_reader.to_reader,
                "schema": lambda: flight_reader.schema
            }.get(mode, flight_reader.read_all)

            return mode_func() if callable(mode_func) else mode_func
        except Exception as e:
            raise e

    def close(self):
        """Close the client and clean up resources."""
        self._write_api.close()
        self._flight_client.close()
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
    "flight_client_options"
]
