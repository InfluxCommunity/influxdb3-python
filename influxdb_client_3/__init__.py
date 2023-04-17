# influxdb_client_3/__init__.py

from pyarrow import csv
import pyarrow.flight as flight
from flightsql import FlightSQLClient
from influxdb_client import InfluxDBClient as _InfluxDBClient
from influxdb_client.client.write_api import WriteApi as _WriteApi
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from influxdb_client import Point

class InfluxDBClient3:
    def __init__(self, host=None, org=None, database=None, token=None, write_options="SYNCHRONOUS", **kwargs):
        """
        This class provides an interface for interacting with an InfluxDB server, simplifying common operations such as writing, querying.
        * host (str, optional): The hostname or IP address of the InfluxDB server. Defaults to None.
        * org (str, optional): The InfluxDB organization name to be used for operations. Defaults to None.
        * database (str, optional): The database to be used for InfluxDB operations. Defaults to None.
        * token (str, optional): The authentication token for accessing the InfluxDB server. Defaults to None.
        * write_options (enum, optional): Specifies the write mode (synchronous or asynchronous) to use when writing data points to InfluxDB. Defaults to SYNCHRONOUS.
        * **kwargs: Additional arguments to be passed to the InfluxDB Client.
        """
        self._org = org
        self._database = database

        if write_options == "SYNCHRONOUS":
            self.write_options = SYNCHRONOUS
        elif write_options == "ASYNCHRONOUS":
            self.write_options = ASYNCHRONOUS
        else:
            raise ValueError("write_options must be SYNCHRONOUS or ASYNCHRONOUS")

     
        self._client = _InfluxDBClient(url=f"https://{host}", token=token, org=self._org, **kwargs )
        self._write_api = _WriteApi(self._client, write_options=self.write_options )
        self._flight_sql_client = FlightSQLClient(host=host,
                                                  token=token,
                                                  metadata={'bucket-name':database})

    def write(self, record=None, **kwargs):
        """
        Write data to InfluxDB.

        :type database: str
        :param record: The data point(s) to write.
        :type record: Point or list of Point objects
        :param kwargs: Additional arguments to pass to the write API.
        """
        try:
            self._write_api.write(bucket=self._database, record=record, **kwargs)
        except Exception as e:
            print(e)
    
    def write_csv(self, csv_file, measurement_name=None, tag_columns = [],timestamp_column = 'time',  **kwargs):
        """
        Write data from a CSV file to InfluxDB.

        :param csv_file: The CSV file to write.
        :type csv_file: str
        :param kwargs: Additional arguments to pass to the write API.
        """
        try:
            atable = csv.read_csv(csv_file, **kwargs)
     
            df = atable.to_pandas()
            print(df)
            self._write_api.write(bucket=self._database, record=df, 
                                  data_frame_measurement_name=measurement_name, 
                                  data_frame_tag_columns=tag_columns,
                                  data_frame_timestamp_column = timestamp_column )
        except Exception as e:
            print(e)
    



    def query(self, sql_query):
        """
        Query data from InfluxDB IOx using FlightSQL.

        :param sql_query: The SQL query string to execute.
        :type sql_query: str
        :return: pyarrow.Table
        """
        query = self._flight_sql_client.execute(sql_query)
        return self._flight_sql_client.do_get(query.endpoints[0].ticket)

    def __del__(self):
        self._write_api.__del__()
        return self._client.__del__()

__all__ = ["InfluxDBClient3", "Point"]
