"""Query data in InfluxDB 3."""
import asyncio
# coding: utf-8
import json

from pyarrow.flight import FlightClient, Ticket, FlightCallOptions, FlightStreamReader

from influxdb_client_3.version import USER_AGENT


class QueryApiOptions(object):
    """
    Structure for encapsulating options for the QueryApi

    Attributes
    ----------
    tls_root_certs (bytes):  contents of an SSL root certificate or chain read as bytes
    tls_verify (bool): whether to verify SSL certificates or not
    proxy (str): URL to a proxy server
    flight_client_options (dict): base set of flight client options passed to internal pyarrow.flight.FlightClient
    """
    tls_root_certs: bytes = None
    tls_verify: bool = None
    proxy: str = None
    flight_client_options: dict = None

    def __init__(self, root_certs_path, verify, proxy, flight_client_options):
        """
        Initialize a set of QueryApiOptions

        :param root_certs_path: path to a certificate .pem file.
        :param verify: whether to verify SSL certificates or not.
        :param proxy: URL of a proxy server, if required.
        :param flight_client_options: set of flight_client_options
               to be passed to internal pyarrow.flight.FlightClient.
        """
        if root_certs_path:
            self.tls_root_certs = self._read_certs(root_certs_path)
        self.tls_verify = verify
        self.proxy = proxy
        self.flight_client_options = flight_client_options

    def _read_certs(self, path):
        with open(path, "rb") as certs_file:
            return certs_file.read()


class QueryApiOptionsBuilder(object):
    """
    Helper class to make adding QueryApiOptions more dynamic.

    Example:

    .. code-block:: python

        options = QueryApiOptionsBuilder()\
            .proxy("http://internal.tunnel.proxy:8080") \
            .root_certs("/home/fred/.etc/ssl/alt_certs.pem") \
            .tls_verify(True) \
            .build()

        client = QueryApi(connection, token, None, None, options)
    """
    _root_certs_path = None
    _tls_verify = True
    _proxy = None
    _flight_client_options = None

    def root_certs(self, path):
        self._root_certs_path = path
        return self

    def tls_verify(self, verify):
        self._tls_verify = verify
        return self

    def proxy(self, proxy):
        self._proxy = proxy
        return self

    def flight_client_options(self, flight_client_options):
        self._flight_client_options = flight_client_options
        return self

    def build(self):
        """Build a QueryApiOptions object with previously set values"""
        return QueryApiOptions(
            root_certs_path=self._root_certs_path,
            verify=self._tls_verify,
            proxy=self._proxy,
            flight_client_options=self._flight_client_options
        )


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
                 flight_client_options,
                 proxy=None, options=None) -> None:
        """
        Initialize defaults.

        :param connection_string: Flight/gRPC connection string
        :param token: access token
        :param flight_client_options: Flight client options
        """
        self._token = token
        self._flight_client_options = flight_client_options or {}
        default_user_agent = ("grpc.secondary_user_agent", USER_AGENT)
        if "generic_options" in self._flight_client_options:
            if "grpc.secondary_user_agent" not in dict(self._flight_client_options["generic_options"]).keys():
                self._flight_client_options["generic_options"].append(default_user_agent)
        else:
            self._flight_client_options["generic_options"] = [default_user_agent]
        self._proxy = proxy
        from influxdb_client_3 import _merge_options as merge_options
        if options:
            if options.flight_client_options:
                self._flight_client_options = merge_options(self._flight_client_options,
                                                            None,
                                                            options.flight_client_options)
                if ('generic_options' in options.flight_client_options and
                        'grpc.secondary_user_agent' in dict(options.flight_client_options["generic_options"]).keys()):
                    self._flight_client_options['generic_options'].remove(default_user_agent)
            if options.tls_root_certs:
                self._flight_client_options["tls_root_certs"] = options.tls_root_certs
            if options.proxy:
                self._proxy = options.proxy
            if options.tls_verify is not None:
                self._flight_client_options["disable_server_verification"] = not options.tls_verify
        if self._proxy:
            self._flight_client_options["generic_options"].append(("grpc.http_proxy", self._proxy))
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
        try:
            ticket, _options = self._prepare_query(query, language, database, **kwargs)

            flight_reader = self._do_get(ticket, _options)

            return self._translate_stream_reader(flight_reader, mode)
        except Exception as e:
            raise e

    async def query_async(self, query: str, language: str, mode: str, database: str, **kwargs):
        """Query data from InfluxDB asynchronously.

        Wraps internal FlightClient.doGet call in its own executor, so that the event_loop will not be blocked.

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
        try:
            ticket, options = self._prepare_query(query, language, database, **kwargs)
            loop = asyncio.get_running_loop()
            _flight_reader = await loop.run_in_executor(None,
                                                        self._flight_client.do_get, ticket, options)
            return await loop.run_in_executor(None, self._translate_stream_reader,
                                              _flight_reader,
                                              mode)
        except Exception as e:
            raise e

    def _translate_stream_reader(self, reader: FlightStreamReader, mode: str):
        from influxdb_client_3 import polars as has_polars
        try:
            mode_funcs = {
                "all": reader.read_all,
                "pandas": reader.read_pandas,
                "chunk": lambda: reader,
                "reader": reader.to_reader,
                "schema": lambda: reader.schema
            }
            if has_polars:
                import polars as pl
                mode_funcs["polars"] = lambda: pl.from_arrow(reader.read_all())
            mode_func = mode_funcs.get(mode, reader.read_all)

            return mode_func() if callable(mode_func) else mode_func
        except Exception as e:
            raise e

    def _prepare_query(self, query: str, language: str, database: str, **kwargs):
        from influxdb_client_3 import _merge_options as merge_options
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

        return ticket, _options

    def _do_get(self, ticket: Ticket, options: FlightCallOptions = None) -> FlightStreamReader:
        return self._flight_client.do_get(ticket, options)

    def close(self):
        """Close the Flight client."""
        self._flight_client.close()
