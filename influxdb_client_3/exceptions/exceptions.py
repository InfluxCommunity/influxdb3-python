"""Exception utils for InfluxDB."""
from __future__ import absolute_import

import logging

from urllib3 import HTTPResponse

logger = logging.getLogger('influxdb_client_3.exceptions')


class InfluxDB3ClientException(Exception):
    """
    Exception raised for errors in the InfluxDB client operations.

    Represents errors that occur during interactions with the InfluxDB
    database client. This exception is a general base class for more
    specific client-related failures and is typically used to signal issues
    such as invalid queries, connection failures, or API misusage.
    """
    pass


# This error is for all query operations
class InfluxDB3ClientQueryException(InfluxDB3ClientException):
    """
    Represents an error that occurs when querying an InfluxDB client.

    This class is specifically designed to handle errors originating from
    client queries to an InfluxDB database. It extends the general
    `InfluxDB3ClientException`, allowing more precise identification and
    handling of query-related issues.

    :ivar message: Contains the specific error message describing the
        query error.
    :type message: str
    """

    def __init__(self, error_message, *args, **kwargs):
        super().__init__(error_message, *args, **kwargs)
        self.message = error_message


class InfluxDBRestClientException(InfluxDB3ClientException):
    def __init__(self, http_resp: HTTPResponse = None, status: int = None, message: str = None, reason: str = None):
        super().__init__()
        if http_resp:
            self.status = http_resp.status
            self.reason = http_resp.reason
            self.body = http_resp.data
            self.message = message or ''
            self.headers = http_resp.getheaders()
            self.response = http_resp
        else:
            self.status = status
            self.reason = reason
            self.body = None
            self.headers = None
            self.message = message or 'no response'
            self.response = None

    def getheaders(self):
        """Helper method to make response headers more accessible."""
        return self.response.getheaders()

    def __str__(self):
        """Get custom error messages for exception."""
        error_message = "({0})\n" \
                        "Reason: {1}\n".format(self.status, self.reason)
        if self.headers:
            error_message += "HTTP response headers: {0}\n".format(
                self.headers)

        if self.body:
            error_message += "HTTP response body: {0}\n".format(self.body)

        return error_message
