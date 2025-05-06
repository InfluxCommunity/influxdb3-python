# coding: utf-8

from __future__ import absolute_import

import logging
from typing import Dict

from influxdb_client_3.write_client.client.exceptions import InfluxDBError
from influxdb_client_3.write_client.configuration import Configuration

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


class _BaseRESTClient(object):
    logger = logging.getLogger('influxdb_client.client.http')

    @staticmethod
    def log_request(method: str, url: str):
        _BaseRESTClient.logger.debug(f">>> Request: '{method} {url}'")

    @staticmethod
    def log_response(status: str):
        _BaseRESTClient.logger.debug(f"<<< Response: {status}")

    @staticmethod
    def log_body(body: object, prefix: str):
        _BaseRESTClient.logger.debug(f"{prefix} Body: {body}")

    @staticmethod
    def log_headers(headers: Dict[str, str], prefix: str):
        for key, v in headers.items():
            value = v
            if 'authorization' == key.lower():
                value = '***'
            _BaseRESTClient.logger.debug(f"{prefix} {key}: {value}")


def _requires_create_user_session(configuration: Configuration, cookie: str, resource_path: str):
    _unauthorized = ['/api/v2/signin', '/api/v2/signout']
    return configuration.username and configuration.password and not cookie and resource_path not in _unauthorized


def _requires_expire_user_session(configuration: Configuration, cookie: str):
    return configuration.username and configuration.password and cookie
