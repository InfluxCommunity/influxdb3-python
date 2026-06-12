# coding: utf-8

from __future__ import absolute_import

import io
import logging
import multiprocessing
import ssl
import sys
from typing import Dict
from urllib.parse import urlencode

from influxdb_client_3.write_client.write_exceptions import ApiException

try:
    import urllib3
except ImportError as e:
    raise ImportError('urllib3 is required to use influxdb3-python.') from e


class RESTResponse(io.IOBase):

    def __init__(self, resp):
        """Initialize with HTTP response."""
        self.urllib3_response = resp
        self.status = resp.status
        self.reason = resp.reason
        self.data = resp.data

    def getheaders(self):
        """Return a dictionary of the response headers."""
        return self.urllib3_response.headers

    def getheader(self, name, default=None):
        """Return a given response header."""
        return self.urllib3_response.headers.get(name, default)


class RestClient(object):
    logger = logging.getLogger('influxdb_client.client.http')

    def __init__(self,
                 base_url,
                 default_header=None,
                 verify_ssl=True,
                 ssl_ca_cert=None,
                 cert_file=None,
                 cert_key_file=None,
                 cert_key_password=None,
                 ssl_context=None,
                 proxy=None,
                 proxy_headers=None,
                 pools_size=4,
                 maxsize=None,
                 timeout=None,
                 retries=False,
                 debug=False,
                 connection_pool_maxsize=multiprocessing.cpu_count() * 5,
                 ):
        """Initialize REST client."""
        # urllib3.PoolManager will pass all kw parameters to connectionpool
        # https://github.com/shazow/urllib3/blob/f9409436f83aeb79fbaf090181cd81b784f1b8ce/urllib3/poolmanager.py#L75  # noqa: E501
        # https://github.com/shazow/urllib3/blob/f9409436f83aeb79fbaf090181cd81b784f1b8ce/urllib3/connectionpool.py#L680  # noqa: E501
        # maxsize is the number of requests to host that are allowed in parallel  # noqa: E501
        # Custom SSL certificates and client certificates: http://urllib3.readthedocs.io/en/latest/advanced-usage.html  # noqa: E501

        self.base_url = base_url
        self.pools_size = pools_size
        self.maxsize = maxsize
        self.timeout = timeout
        self.retries = retries
        self.default_header = default_header
        self.verify_ssl = verify_ssl
        self.ssl_context = ssl_context
        self.proxy = proxy
        self.proxy_headers = proxy_headers
        self.ssl_ca_cert = ssl_ca_cert
        self.cert_file = cert_file
        self.cert_key_file = cert_key_file
        self.cert_key_password = cert_key_password
        self.debug = debug
        self.connection_pool_maxsize = connection_pool_maxsize

        # cert_reqs
        if verify_ssl:
            cert_reqs = ssl.CERT_REQUIRED
        else:
            cert_reqs = ssl.CERT_NONE

        # ca_certs
        if ssl_ca_cert:
            ca_certs = ssl_ca_cert
        else:
            ca_certs = None

        addition_pool_args = {'retries': self.retries}

        if maxsize is None:
            if connection_pool_maxsize is not None:
                maxsize = connection_pool_maxsize
            else:
                maxsize = 4

        # https pool manager
        if proxy:
            self.pool_manager = urllib3.ProxyManager(
                num_pools=pools_size,
                maxsize=maxsize,
                cert_reqs=cert_reqs,
                ca_certs=ca_certs,
                cert_file=cert_file,
                key_file=cert_key_file,
                key_password=cert_key_password,
                proxy_url=proxy,
                proxy_headers=proxy_headers,
                ssl_context=ssl_context,
                **addition_pool_args
            )
        else:
            self.pool_manager = urllib3.PoolManager(
                num_pools=pools_size,
                maxsize=maxsize,
                cert_reqs=cert_reqs,
                ca_certs=ca_certs,
                cert_file=cert_file,
                key_file=cert_key_file,
                key_password=cert_key_password,
                ssl_context=ssl_context,
                **addition_pool_args
            )

    def request(self, method, path, query_params=None, headers=None,
                body=None, timeout=None, **urlopen_kw):
        """Perform requests.

        :param method: http request method
        :param path: http request path
        :param query_params: query parameters in the url
        :param headers: http request headers
        :param body: request json body, for `application/json`
        :param timeout: timeout setting for this request. If one
                                 number is provided, it will be a total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :param urlopen_kw: Additional parameters are passed to
                           :meth:`urllib3.request.RequestMethods.request`
        """

        url = self.base_url + path
        if query_params:
            url = url + '?' + urlencode(query_params)
        merged_headers = {}
        if self.default_header:
            merged_headers.update(self.default_header)
        if headers:
            merged_headers.update(headers)

        effective_timeout = timeout if timeout is not None else self.timeout

        if self.debug:
            RestClient.log_request(method, url)
            RestClient.log_headers(merged_headers, '>>>')
            RestClient.log_body(body, '>>>')

        try:
            r = self.pool_manager.request(
                method, url=url,
                body=body,
                headers=merged_headers,
                timeout=effective_timeout,
                **urlopen_kw
            )
        except urllib3.exceptions.SSLError as e:
            msg = "{0}\n{1}".format(type(e).__name__, str(e))
            raise ApiException(status=0, reason=msg)

        r = RESTResponse(r)
        r.data = r.data.decode('utf8')

        if self.debug:
            RestClient.log_response(r.status)
            if hasattr(r, 'headers'):
                RestClient.log_headers(r.headers, '<<<')
            if hasattr(r, 'urllib3_response'):
                RestClient.log_headers(r.urllib3_response.headers, '<<<')
            RestClient.log_body(r.data, '<<<')

        if not 200 <= r.status <= 299:
            raise ApiException(http_resp=r)

        return r

    @property
    def debug(self):
        """Debug status.

        :param value: The debug status, True or False.
        :type: bool
        """
        return self.__debug

    @debug.setter
    def debug(self, value):
        """Debug status.

        :param value: The debug status, True or False.
        :type: bool
        """
        self.__debug = value
        if self.__debug:
            # if debug status is True, turn on debug logging
            self.logger.setLevel(logging.DEBUG)
            if not any(map(lambda h: isinstance(h, logging.StreamHandler) and h.stream == sys.stdout,
                           self.logger.handlers)):
                self.logger.addHandler(logging.StreamHandler(sys.stdout))
            # we use 'influxdb_client.client.http' logger instead of this
            # httplib.HTTPConnection.debuglevel = 1
        else:
            # if debug status is False, turn off debug logging,
            # setting log level to default `logging.WARNING`
            self.logger.setLevel(logging.WARNING)
            # we use 'influxdb_client.client.http' logger instead of this
            # httplib.HTTPConnection.debuglevel = 0

    @staticmethod
    def log_request(method: str, url: str):
        RestClient.logger.debug(f">>> Request: '{method} {url}'")

    @staticmethod
    def log_response(status: str):
        RestClient.logger.debug(f"<<< Response: {status}")

    @staticmethod
    def log_body(body: object, prefix: str):
        RestClient.logger.debug(f"{prefix} Body: {body}")

    @staticmethod
    def log_headers(headers: Dict[str, str], prefix: str):
        for key, v in headers.items():
            value = v
            if 'authorization' == key.lower():
                value = '***'
            RestClient.logger.debug(f"{prefix} {key}: {value}")

    def close(self):
        self.pool_manager.clear()

    def __getstate__(self):
        """Return a dict of attributes that you want to pickle."""
        state = self.__dict__.copy()
        # Remove Pool manager
        del state['pool_manager']
        return state

    def __setstate__(self, state):
        """Set your object with the provided dict."""
        self.__dict__.update(state)
        # Init Pool manager
        self.__init__(
            base_url=self.base_url,
            pools_size=self.pools_size,
            maxsize=self.maxsize,
            timeout=self.timeout,
            retries=self.retries,
            default_header=self.default_header,
            verify_ssl=self.verify_ssl,
            ssl_context=self.ssl_context,
            proxy=self.proxy,
            proxy_headers=self.proxy_headers,
            ssl_ca_cert=self.ssl_ca_cert,
            cert_file=self.cert_file,
            cert_key_file=self.cert_key_file,
            cert_key_password=self.cert_key_password,
            debug=self.debug,
            connection_pool_maxsize=self.connection_pool_maxsize
        )
