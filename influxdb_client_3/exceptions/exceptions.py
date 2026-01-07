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

                # Check for new format with line error details (data as array)
                if isinstance(node.get('data'), list) and 'error' in node:
                    try:
                        # Extract main error message
                        main_error = node['error']

                        # Parse line errors
                        line_errors = []
                        for item in node['data']:
                            line_error = {}
                            if 'error_message' in item:
                                line_error['error_message'] = item['error_message']
                            if 'line_number' in item:
                                line_error['line_number'] = item['line_number']
                            if 'original_line' in item:
                                line_error['original_line'] = item['original_line']
                            if line_error:  # Only add if we extracted at least one field
                                line_errors.append(line_error)

                        # Build formatted message
                        message_parts = [main_error]
                        for err in line_errors:
                            line_num = err.get('line_number', '?')
                            err_msg = err.get('error_message', 'Unknown error')
                            orig_line = err.get('original_line', '')
                            message_parts.append(f"Line {line_num}: {err_msg}")
                            if orig_line:
                                message_parts.append(f"  Original: {orig_line}")

                        return '\n'.join(message_parts)
                    except Exception as e:
                        logging.debug(f"Cannot parse line error details: {e}")
                        # Fall through to existing logic

                # Existing logic for other error formats
                for key in [['message'], ['error']]:
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
