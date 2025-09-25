"""Exceptions utils for InfluxDB."""

import logging

from urllib3 import HTTPResponse

logger = logging.getLogger('influxdb_client_3.exceptions')


class InfluxDB3ClientError(Exception):
    """
    Exception raised for errors in the InfluxDB client operations.

    Represents errors that occur during interactions with the InfluxDB
    database client. This exception is a general base class for more
    specific client-related failures and is typically used to signal issues
    such as invalid queries, connection failures, or API misusage.
    """
    pass


# This error is for all query operations
class InfluxDB3ClientQueryError(InfluxDB3ClientError):
    """
    Represents an error that occurs when querying an InfluxDB client.

    This class is specifically designed to handle errors originating from
    client queries to an InfluxDB database. It extends the general
    `InfluxDBClientError`, allowing more precise identification and
    handling of query-related issues.

    :ivar message: Contains the specific error message describing the
        query error.
    :type message: str
    """

    def __init__(self, error_message, *args, **kwargs):
        super().__init__(error_message, *args, **kwargs)
        self.message = error_message


# This error is for all write operations
class InfluxDBError(InfluxDB3ClientError):
    """Raised when a server error occurs."""

    def __init__(self, response: HTTPResponse = None, message: str = None):
        """Initialize the InfluxDBError handler."""
        if response is not None:
            self.response = response
            self.message = self._get_message(response)
            self.retry_after = response.getheader('Retry-After')
        else:
            self.response = None
            self.message = message or 'no response'
            self.retry_after = None
        super().__init__(self.message)

    def _get_message(self, response):
        # Body
        if response.data:
            import json

            def get(d, key):
                if not key or d is None:
                    return d
                return get(d.get(key[0]), key[1:])
            try:
                node = json.loads(response.data)
                for key in [['message'], ['data', 'error_message'], ['error']]:
                    value = get(node, key)
                    if value is not None:
                        return value
                return response.data
            except Exception as e:
                logging.debug(f"Cannot parse error response to JSON: {response.data}, {e}")
                return response.data

        # Header
        for header_key in ["X-Platform-Error-Code", "X-Influx-Error", "X-InfluxDb-Error"]:
            header_value = response.getheader(header_key)
            if header_value is not None:
                return header_value

        # Http Status
        return response.reason

    def getheaders(self):
        """Helper method to make response headers more accessible."""
        return self.response.getheaders()
