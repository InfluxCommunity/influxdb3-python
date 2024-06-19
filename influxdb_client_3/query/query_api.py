"""Query data in InfluxDB 3."""

# coding: utf-8
import json

from pyarrow.flight import FlightClient, Ticket, FlightCallOptions, FlightStreamReader
from influxdb_client_3.version import USER_AGENT


class QueryApi(object):
    """
    Implementation for '/api/v2/query' endpoint.

    Example:
        .. code-block:: python

            from influxdb_client import InfluxDBClient


            # Initialize instance of QueryApi
            with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
                query_api = client.query_api()
    """

    def __init__(self,
                 connection_string,
                 token,
                 flight_client_options) -> None:
        """
        Initialize defaults.

        :param connection_string: Flight/gRPC connection string
        :param token: access token
        :param flight_client_options: Flight client options
        """
        self._token = token
        self._flight_client_options = flight_client_options or {}
        self._flight_client_options["generic_options"] = [
            ("grpc.secondary_user_agent", USER_AGENT)
        ]
        self._flight_client = FlightClient(connection_string, **self._flight_client_options)

    def query(self, query: str, language: str, mode: str, database: str, **kwargs):
        """Query data from InfluxDB.

        :param query: The query to execute on the database.
        :param language: The query language.
        :param mode: The mode to use for the query.
                     It should be one of "all", "pandas", "polars", "chunk", "reader" or "schema".
        :param database: The database to query from.
        :param kwargs: Additional arguments to pass to the ``FlightCallOptions headers``.
                       For example, it can be used to set up per request headers.
        :keyword query_parameters: The query parameters to use in the query.
                                   It should be a ``dictionary`` of key-value pairs.
        :return: The query result in the specified mode.
        """
        from influxdb_client_3 import polars as has_polars, _merge_options as merge_options
        try:
            # Create an authorization header
            optargs = {
                "headers": [(b"authorization", f"Bearer {self._token}".encode('utf-8'))],
                "timeout": 300
            }
            opts = merge_options(optargs, exclude_keys=['query_parameters'], custom=kwargs)
            _options = FlightCallOptions(**opts)

            #
            # Ticket data
            #
            ticket_data = {
                "database": database,
                "sql_query": query,
                "query_type": language
            }
            # add query parameters
            query_parameters = kwargs.get("query_parameters", None)
            if query_parameters:
                ticket_data["params"] = query_parameters

            ticket = Ticket(json.dumps(ticket_data).encode('utf-8'))
            flight_reader = self._do_get(ticket, _options)

            mode_funcs = {
                "all": flight_reader.read_all,
                "pandas": flight_reader.read_pandas,
                "chunk": lambda: flight_reader,
                "reader": flight_reader.to_reader,
                "schema": lambda: flight_reader.schema
            }
            if has_polars:
                import polars as pl
                mode_funcs["polars"] = lambda: pl.from_arrow(flight_reader.read_all())
            mode_func = mode_funcs.get(mode, flight_reader.read_all)

            return mode_func() if callable(mode_func) else mode_func
        except Exception as e:
            raise e

    def _do_get(self, ticket: Ticket, options: FlightCallOptions = None) -> FlightStreamReader:
        return self._flight_client.do_get(ticket, options)

    def close(self):
        """Close the Flight client."""
        self._flight_client.close()
