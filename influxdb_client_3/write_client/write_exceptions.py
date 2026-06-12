# coding: utf-8

from __future__ import absolute_import

from influxdb_client_3.exceptions import InfluxDBError

_UTF_8_encoding = 'utf-8'


class ApiException(InfluxDBError):

    def __init__(self, status=None, reason=None, http_resp=None):
        """Initialize with HTTP response."""
        super().__init__(response=http_resp)
        if http_resp:
            self.status = http_resp.status
            self.reason = http_resp.reason
            self.body = http_resp.data
            self.headers = http_resp.getheaders()
        else:
            self.status = status
            self.reason = reason
            self.body = None
            self.headers = None

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
