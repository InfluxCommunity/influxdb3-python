# coding: utf-8

from __future__ import absolute_import

import re  # noqa: F401

from influxdb_client_3.write_client.service._base_service import _BaseService


class SigninService(_BaseService):

    def __init__(self, api_client=None):  # noqa: E501,D401,D403
        """SigninService - a operation defined in OpenAPI."""
        super().__init__(api_client)

    def post_signin(self, **kwargs):  # noqa: E501,D401,D403
        """Create a user session..

        Authenticates [Basic authentication credentials](#section/Authentication/BasicAuthentication) for a [user](https://docs.influxdata.com/influxdb/latest/reference/glossary/#user), and then, if successful, generates a user session.  To authenticate a user, pass the HTTP `Authorization` header with the `Basic` scheme and the base64-encoded username and password. For syntax and more information, see [Basic Authentication](#section/Authentication/BasicAuthentication) for syntax and more information.  If authentication is successful, InfluxDB creates a new session for the user and then returns the session cookie in the `Set-Cookie` response header.  InfluxDB stores user sessions in memory only. They expire within ten minutes and during restarts of the InfluxDB instance.  #### User sessions with authorizations  - In InfluxDB Cloud, a user session inherits all the user's permissions for   the organization. - In InfluxDB OSS, a user session inherits all the user's permissions for all   the organizations that the user belongs to.  #### Related endpoints  - [Signout](#tag/Signout)
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.post_signin(async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str zap_trace_span: OpenTracing span context
        :param str authorization: An auth credential for the Basic scheme
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """  # noqa: E501
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.post_signin_with_http_info(**kwargs)  # noqa: E501
        else:
            (data) = self.post_signin_with_http_info(**kwargs)  # noqa: E501
            return data

    def post_signin_with_http_info(self, **kwargs):  # noqa: E501,D401,D403
        """Create a user session..

        Authenticates [Basic authentication credentials](#section/Authentication/BasicAuthentication) for a [user](https://docs.influxdata.com/influxdb/latest/reference/glossary/#user), and then, if successful, generates a user session.  To authenticate a user, pass the HTTP `Authorization` header with the `Basic` scheme and the base64-encoded username and password. For syntax and more information, see [Basic Authentication](#section/Authentication/BasicAuthentication) for syntax and more information.  If authentication is successful, InfluxDB creates a new session for the user and then returns the session cookie in the `Set-Cookie` response header.  InfluxDB stores user sessions in memory only. They expire within ten minutes and during restarts of the InfluxDB instance.  #### User sessions with authorizations  - In InfluxDB Cloud, a user session inherits all the user's permissions for   the organization. - In InfluxDB OSS, a user session inherits all the user's permissions for all   the organizations that the user belongs to.  #### Related endpoints  - [Signout](#tag/Signout)
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.post_signin_with_http_info(async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str zap_trace_span: OpenTracing span context
        :param str authorization: An auth credential for the Basic scheme
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """  # noqa: E501
        local_var_params, path_params, query_params, header_params, body_params = \
            self._post_signin_prepare(**kwargs)  # noqa: E501

        return self.api_client.call_api(
            '/api/v2/signin', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=[],
            files={},
            response_type=None,  # noqa: E501
            auth_settings=['BasicAuthentication'],
            async_req=local_var_params.get('async_req'),
            _return_http_data_only=local_var_params.get('_return_http_data_only'),  # noqa: E501
            _preload_content=local_var_params.get('_preload_content', True),
            _request_timeout=local_var_params.get('_request_timeout'),
            collection_formats={},
            urlopen_kw=kwargs.get('urlopen_kw', None))

    async def post_signin_async(self, **kwargs):  # noqa: E501,D401,D403
        """Create a user session..

        Authenticates [Basic authentication credentials](#section/Authentication/BasicAuthentication) for a [user](https://docs.influxdata.com/influxdb/latest/reference/glossary/#user), and then, if successful, generates a user session.  To authenticate a user, pass the HTTP `Authorization` header with the `Basic` scheme and the base64-encoded username and password. For syntax and more information, see [Basic Authentication](#section/Authentication/BasicAuthentication) for syntax and more information.  If authentication is successful, InfluxDB creates a new session for the user and then returns the session cookie in the `Set-Cookie` response header.  InfluxDB stores user sessions in memory only. They expire within ten minutes and during restarts of the InfluxDB instance.  #### User sessions with authorizations  - In InfluxDB Cloud, a user session inherits all the user's permissions for   the organization. - In InfluxDB OSS, a user session inherits all the user's permissions for all   the organizations that the user belongs to.  #### Related endpoints  - [Signout](#tag/Signout)
        This method makes an asynchronous HTTP request.

        :param async_req bool
        :param str zap_trace_span: OpenTracing span context
        :param str authorization: An auth credential for the Basic scheme
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """  # noqa: E501
        local_var_params, path_params, query_params, header_params, body_params = \
            self._post_signin_prepare(**kwargs)  # noqa: E501

        return await self.api_client.call_api(
            '/api/v2/signin', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=[],
            files={},
            response_type=None,  # noqa: E501
            auth_settings=['BasicAuthentication'],
            async_req=local_var_params.get('async_req'),
            _return_http_data_only=local_var_params.get('_return_http_data_only'),  # noqa: E501
            _preload_content=local_var_params.get('_preload_content', True),
            _request_timeout=local_var_params.get('_request_timeout'),
            collection_formats={},
            urlopen_kw=kwargs.get('urlopen_kw', None))

    def _post_signin_prepare(self, **kwargs):  # noqa: E501,D401,D403
        local_var_params = locals()

        all_params = ['zap_trace_span', 'authorization']  # noqa: E501
        self._check_operation_params('post_signin', all_params, local_var_params)

        path_params = {}

        query_params = []

        header_params = {}
        if 'zap_trace_span' in local_var_params:
            header_params['Zap-Trace-Span'] = local_var_params['zap_trace_span']  # noqa: E501
        if 'authorization' in local_var_params:
            header_params['Authorization'] = local_var_params['authorization']  # noqa: E501

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        return local_var_params, path_params, query_params, header_params, body_params
