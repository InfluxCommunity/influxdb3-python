# coding: utf-8

from __future__ import absolute_import

import io
import json
import re
import ssl
from urllib.parse import urlencode

from influxdb_client_3.write_client.rest import ApiException
from influxdb_client_3.write_client.rest import _BaseRESTClient

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


class RESTClientObject(object):

    def __init__(self, configuration, pools_size=4, maxsize=None, retries=False):
        """Initialize REST client."""
        # urllib3.PoolManager will pass all kw parameters to connectionpool
        # https://github.com/shazow/urllib3/blob/f9409436f83aeb79fbaf090181cd81b784f1b8ce/urllib3/poolmanager.py#L75  # noqa: E501
        # https://github.com/shazow/urllib3/blob/f9409436f83aeb79fbaf090181cd81b784f1b8ce/urllib3/connectionpool.py#L680  # noqa: E501
        # maxsize is the number of requests to host that are allowed in parallel  # noqa: E501
        # Custom SSL certificates and client certificates: http://urllib3.readthedocs.io/en/latest/advanced-usage.html  # noqa: E501

        self.configuration = configuration
        self.pools_size = pools_size
        self.maxsize = maxsize
        self.retries = retries

        # cert_reqs
        if configuration.verify_ssl:
            cert_reqs = ssl.CERT_REQUIRED
        else:
            cert_reqs = ssl.CERT_NONE

        # ca_certs
        if configuration.ssl_ca_cert:
            ca_certs = configuration.ssl_ca_cert
        else:
            ca_certs = None

        addition_pool_args = {}
        if configuration.assert_hostname is not None:
            addition_pool_args['assert_hostname'] = configuration.assert_hostname  # noqa: E501
        addition_pool_args['retries'] = self.retries

        if maxsize is None:
            if configuration.connection_pool_maxsize is not None:
                maxsize = configuration.connection_pool_maxsize
            else:
                maxsize = 4

        # https pool manager
        if configuration.proxy:
            self.pool_manager = urllib3.ProxyManager(
                num_pools=pools_size,
                maxsize=maxsize,
                cert_reqs=cert_reqs,
                ca_certs=ca_certs,
                cert_file=configuration.cert_file,
                key_file=configuration.cert_key_file,
                key_password=configuration.cert_key_password,
                proxy_url=configuration.proxy,
                proxy_headers=configuration.proxy_headers,
                ssl_context=configuration.ssl_context,
                **addition_pool_args
            )
        else:
            self.pool_manager = urllib3.PoolManager(
                num_pools=pools_size,
                maxsize=maxsize,
                cert_reqs=cert_reqs,
                ca_certs=ca_certs,
                cert_file=configuration.cert_file,
                key_file=configuration.cert_key_file,
                key_password=configuration.cert_key_password,
                ssl_context=configuration.ssl_context,
                **addition_pool_args
            )

    def request(self, method, url, query_params=None, headers=None,
                body=None, post_params=None, _preload_content=True,
                _request_timeout=None, **urlopen_kw):
        """Perform requests.

        :param method: http request method
        :param url: http request url
        :param query_params: query parameters in the url
        :param headers: http request headers
        :param body: request json body, for `application/json`
        :param post_params: request post parameters,
                            `application/x-www-form-urlencoded`
                            and `multipart/form-data`
        :param _preload_content: if False, the urllib3.HTTPResponse object will
                                 be returned without reading/decoding response
                                 data. Default is True.
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :param urlopen_kw: Additional parameters are passed to
                           :meth:`urllib3.request.RequestMethods.request`
        """
        method = method.upper()
        assert method in ['GET', 'HEAD', 'DELETE', 'POST', 'PUT',
                          'PATCH', 'OPTIONS']

        if post_params and body:
            raise ValueError(
                "body parameter cannot be used with post_params parameter."
            )

        post_params = post_params or {}
        headers = headers or {}

        timeout = None
        _configured_timeout = _request_timeout or self.configuration.timeout
        if _configured_timeout:
            if isinstance(_configured_timeout, (int, float, )):  # noqa: E501,F821
                timeout = urllib3.Timeout(total=_configured_timeout / 1_000)
            elif (isinstance(_configured_timeout, tuple) and
                  len(_configured_timeout) == 2):
                timeout = urllib3.Timeout(
                    connect=_configured_timeout[0] / 1_000, read=_configured_timeout[1] / 1_000)

        if 'Content-Type' not in headers:
            headers['Content-Type'] = 'application/json'

        if self.configuration.debug:
            _BaseRESTClient.log_request(method, f"{url}?{urlencode(query_params)}")
            _BaseRESTClient.log_headers(headers, '>>>')
            _BaseRESTClient.log_body(body, '>>>')

        try:
            # For `POST`, `PUT`, `PATCH`, `OPTIONS`, `DELETE`
            if method in ['POST', 'PUT', 'PATCH', 'OPTIONS', 'DELETE']:
                if query_params:
                    url += '?' + urlencode(query_params)
                if re.search('json', headers['Content-Type'], re.IGNORECASE):
                    request_body = None
                    if body is not None:
                        request_body = json.dumps(body)
                    r = self.pool_manager.request(
                        method, url,
                        body=request_body,
                        preload_content=_preload_content,
                        timeout=timeout,
                        headers=headers,
                        **urlopen_kw)
                elif headers['Content-Type'] == 'application/x-www-form-urlencoded':  # noqa: E501
                    r = self.pool_manager.request(
                        method, url,
                        fields=post_params,
                        encode_multipart=False,
                        preload_content=_preload_content,
                        timeout=timeout,
                        headers=headers,
                        **urlopen_kw)
                elif headers['Content-Type'] == 'multipart/form-data':
                    # must del headers['Content-Type'], or the correct
                    # Content-Type which generated by urllib3 will be
                    # overwritten.
                    del headers['Content-Type']
                    r = self.pool_manager.request(
                        method, url,
                        fields=post_params,
                        encode_multipart=True,
                        preload_content=_preload_content,
                        timeout=timeout,
                        headers=headers,
                        **urlopen_kw)
                # Pass a `string` parameter directly in the body to support
                # other content types than Json when `body` argument is
                # provided in serialized form
                elif isinstance(body, str) or isinstance(body, bytes):
                    request_body = body
                    r = self.pool_manager.request(
                        method, url,
                        body=request_body,
                        preload_content=_preload_content,
                        timeout=timeout,
                        headers=headers,
                        **urlopen_kw)
                else:
                    # Cannot generate the request from given parameters
                    msg = """Cannot prepare a request message for provided
                             arguments. Please check that your arguments match
                             declared content type."""
                    raise ApiException(status=0, reason=msg)
            # For `GET`, `HEAD`
            else:
                r = self.pool_manager.request(method, url,
                                              fields=query_params,
                                              preload_content=_preload_content,
                                              timeout=timeout,
                                              headers=headers,
                                              **urlopen_kw)
        except urllib3.exceptions.SSLError as e:
            msg = "{0}\n{1}".format(type(e).__name__, str(e))
            raise ApiException(status=0, reason=msg)

        if _preload_content:
            r = RESTResponse(r)

            # In the python 3, the response.data is bytes.
            # we need to decode it to string.
            r.data = r.data.decode('utf8')

        if self.configuration.debug:
            _BaseRESTClient.log_response(r.status)
            if hasattr(r, 'headers'):
                _BaseRESTClient.log_headers(r.headers, '<<<')
            if hasattr(r, 'urllib3_response'):
                _BaseRESTClient.log_headers(r.urllib3_response.headers, '<<<')
            _BaseRESTClient.log_body(r.data, '<<<')

        if not 200 <= r.status <= 299:
            raise ApiException(http_resp=r)

        return r

    def GET(self, url, headers=None, query_params=None, _preload_content=True,
            _request_timeout=None, **urlopen_kw):
        """Perform GET HTTP request."""
        return self.request("GET", url,
                            headers=headers,
                            _preload_content=_preload_content,
                            _request_timeout=_request_timeout,
                            query_params=query_params,
                            **urlopen_kw)

    def HEAD(self, url, headers=None, query_params=None, _preload_content=True,
             _request_timeout=None, **urlopen_kw):
        """Perform HEAD HTTP request."""
        return self.request("HEAD", url,
                            headers=headers,
                            _preload_content=_preload_content,
                            _request_timeout=_request_timeout,
                            query_params=query_params,
                            **urlopen_kw)

    def OPTIONS(self, url, headers=None, query_params=None, post_params=None,
                body=None, _preload_content=True, _request_timeout=None, **urlopen_kw):
        """Perform OPTIONS HTTP request."""
        return self.request("OPTIONS", url,
                            headers=headers,
                            query_params=query_params,
                            post_params=post_params,
                            _preload_content=_preload_content,
                            _request_timeout=_request_timeout,
                            body=body,
                            **urlopen_kw)

    def DELETE(self, url, headers=None, query_params=None, body=None,
               _preload_content=True, _request_timeout=None, **urlopen_kw):
        """Perform DELETE HTTP request."""
        return self.request("DELETE", url,
                            headers=headers,
                            query_params=query_params,
                            _preload_content=_preload_content,
                            _request_timeout=_request_timeout,
                            body=body,
                            **urlopen_kw)

    def POST(self, url, headers=None, query_params=None, post_params=None,
             body=None, _preload_content=True, _request_timeout=None, **urlopen_kw):
        """Perform POST HTTP request."""
        return self.request("POST", url,
                            headers=headers,
                            query_params=query_params,
                            post_params=post_params,
                            _preload_content=_preload_content,
                            _request_timeout=_request_timeout,
                            body=body,
                            **urlopen_kw)

    def PUT(self, url, headers=None, query_params=None, post_params=None,
            body=None, _preload_content=True, _request_timeout=None, **urlopen_kw):
        """Perform PUT HTTP request."""
        return self.request("PUT", url,
                            headers=headers,
                            query_params=query_params,
                            post_params=post_params,
                            _preload_content=_preload_content,
                            _request_timeout=_request_timeout,
                            body=body,
                            **urlopen_kw)

    def PATCH(self, url, headers=None, query_params=None, post_params=None,
              body=None, _preload_content=True, _request_timeout=None, **urlopen_kw):
        """Perform PATCH HTTP request."""
        return self.request("PATCH", url,
                            headers=headers,
                            query_params=query_params,
                            post_params=post_params,
                            _preload_content=_preload_content,
                            _request_timeout=_request_timeout,
                            body=body,
                            **urlopen_kw)

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
        self.__init__(self.configuration, self.pools_size, self.maxsize, self.retries)
