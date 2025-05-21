class InfluxDBClientError(Exception):
    """
    Exception raised for errors in the InfluxDB client operations.

    Represents errors that occur during interactions with the InfluxDB
    database client. This exception is a general base class for more
    specific client-related failures and is typically used to signal issues
    such as invalid queries, connection failures, or API misusage.
    """
    pass


class InfluxdbClientQueryError(InfluxDBClientError):
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
