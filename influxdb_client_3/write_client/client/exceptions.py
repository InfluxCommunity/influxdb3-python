"""Exceptions utils for InfluxDB."""

import logging

from urllib3 import HTTPResponse

logger = logging.getLogger('influxdb_client_3.write_client.client.exceptions')


class InfluxDBError(Exception):
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
