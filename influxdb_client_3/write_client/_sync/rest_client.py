# coding: utf-8

from __future__ import absolute_import

import io
import multiprocessing
import ssl
from urllib.parse import urlencode

from influxdb_client_3.write_client.rest import ApiException

try:
    import urllib3
except ImportError:
    raise ImportError('OpenAPI Python client requires urllib3.')


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

    def get_string_body(self):
        string = self.urllib3_response.data.decode('utf-8')
        if string is None or string == '':
            return None
        return string


class RestClient(object):

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
            ).connection_from_url(url=base_url)
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
            ).connection_from_url(url=base_url)

    def request(self, method, url, query_params=None, headers=None,
                body=None, timeout=None, **urlopen_kw):
        """Perform requests.

        :param method: http request method
        :param url: http request url
        :param query_params: query parameters in the url
        :param headers: http request headers
        :param body: request json body, for `application/json`
        :param timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :param urlopen_kw: Additional parameters are passed to
                           :meth:`urllib3.request.RequestMethods.request`
        """

        if query_params:
            url += '?' + urlencode(query_params)

        r = self.pool_manager.request(method, url,
                                      body=body,
                                      headers=headers,
                                      timeout=timeout,
                                      **urlopen_kw)

        r = RESTResponse(r)
        r.data = r.data.decode('utf8')

        if not 200 <= r.status <= 299:
            raise ApiException(http_resp=r)

        return r

    def __getstate__(self):
        """Return a dict of attributes that you want to pickle."""
        state = self.__dict__.copy()
        # Remove Pool managaer
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
        )
