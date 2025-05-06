# coding: utf-8

from __future__ import absolute_import

import re  # noqa: F401

from influxdb_client_3.write_client.service._base_service import _BaseService


class SignoutService(_BaseService):

    def __init__(self, api_client=None):  # noqa: E501,D401,D403
        """SignoutService - a operation defined in OpenAPI."""
        super().__init__(api_client)

    def post_signout(self, **kwargs):  # noqa: E501,D401,D403
        """Expire a user session.

        Expires a user session specified by a session cookie.  Use this endpoint to expire a user session that was generated when the user authenticated with the InfluxDB Developer Console (UI) or the `POST /api/v2/signin` endpoint.  For example, the `POST /api/v2/signout` endpoint represents the third step in the following three-step process to authenticate a user, retrieve the `user` resource, and then expire the session:  1. Send a request with the user's [Basic authentication credentials](#section/Authentication/BasicAuthentication)    to the `POST /api/v2/signin` endpoint to create a user session and    generate a session cookie. 2. Send a request to the `GET /api/v2/me` endpoint, passing the stored session cookie    from step 1 to retrieve user information. 3. Send a request to the `POST /api/v2/signout` endpoint, passing the stored session    cookie to expire the session.  _See the complete example in request samples._  InfluxDB stores user sessions in memory only. If a user doesn't sign out, then the user session automatically expires within ten minutes or during a restart of the InfluxDB instance.  To learn more about cookies in HTTP requests, see [Mozilla Developer Network (MDN) Web Docs, HTTP cookies](https://developer.mozilla.org/en-US/docs/Web/HTTP/Cookies).  #### Related endpoints  - [Signin](#tag/Signin)
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.post_signout(async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str zap_trace_span: OpenTracing span context
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """  # noqa: E501
        kwargs['_return_http_data_only'] = True
        if kwargs.get('async_req'):
            return self.post_signout_with_http_info(**kwargs)  # noqa: E501
        else:
            (data) = self.post_signout_with_http_info(**kwargs)  # noqa: E501
            return data

    def post_signout_with_http_info(self, **kwargs):  # noqa: E501,D401,D403
        """Expire a user session.

        Expires a user session specified by a session cookie.  Use this endpoint to expire a user session that was generated when the user authenticated with the InfluxDB Developer Console (UI) or the `POST /api/v2/signin` endpoint.  For example, the `POST /api/v2/signout` endpoint represents the third step in the following three-step process to authenticate a user, retrieve the `user` resource, and then expire the session:  1. Send a request with the user's [Basic authentication credentials](#section/Authentication/BasicAuthentication)    to the `POST /api/v2/signin` endpoint to create a user session and    generate a session cookie. 2. Send a request to the `GET /api/v2/me` endpoint, passing the stored session cookie    from step 1 to retrieve user information. 3. Send a request to the `POST /api/v2/signout` endpoint, passing the stored session    cookie to expire the session.  _See the complete example in request samples._  InfluxDB stores user sessions in memory only. If a user doesn't sign out, then the user session automatically expires within ten minutes or during a restart of the InfluxDB instance.  To learn more about cookies in HTTP requests, see [Mozilla Developer Network (MDN) Web Docs, HTTP cookies](https://developer.mozilla.org/en-US/docs/Web/HTTP/Cookies).  #### Related endpoints  - [Signin](#tag/Signin)
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True
        >>> thread = api.post_signout_with_http_info(async_req=True)
        >>> result = thread.get()

        :param async_req bool
        :param str zap_trace_span: OpenTracing span context
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """  # noqa: E501
        local_var_params, path_params, query_params, header_params, body_params = \
            self._post_signout_prepare(**kwargs)  # noqa: E501

        return self.api_client.call_api(
            '/api/v2/signout', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=[],
            files={},
            response_type=None,  # noqa: E501
            auth_settings=[],
            async_req=local_var_params.get('async_req'),
            _return_http_data_only=local_var_params.get('_return_http_data_only'),  # noqa: E501
            _preload_content=local_var_params.get('_preload_content', True),
            _request_timeout=local_var_params.get('_request_timeout'),
            collection_formats={},
            urlopen_kw=kwargs.get('urlopen_kw', None))

    async def post_signout_async(self, **kwargs):  # noqa: E501,D401,D403
        """Expire a user session.

        Expires a user session specified by a session cookie.  Use this endpoint to expire a user session that was generated when the user authenticated with the InfluxDB Developer Console (UI) or the `POST /api/v2/signin` endpoint.  For example, the `POST /api/v2/signout` endpoint represents the third step in the following three-step process to authenticate a user, retrieve the `user` resource, and then expire the session:  1. Send a request with the user's [Basic authentication credentials](#section/Authentication/BasicAuthentication)    to the `POST /api/v2/signin` endpoint to create a user session and    generate a session cookie. 2. Send a request to the `GET /api/v2/me` endpoint, passing the stored session cookie    from step 1 to retrieve user information. 3. Send a request to the `POST /api/v2/signout` endpoint, passing the stored session    cookie to expire the session.  _See the complete example in request samples._  InfluxDB stores user sessions in memory only. If a user doesn't sign out, then the user session automatically expires within ten minutes or during a restart of the InfluxDB instance.  To learn more about cookies in HTTP requests, see [Mozilla Developer Network (MDN) Web Docs, HTTP cookies](https://developer.mozilla.org/en-US/docs/Web/HTTP/Cookies).  #### Related endpoints  - [Signin](#tag/Signin)
        This method makes an asynchronous HTTP request.

        :param async_req bool
        :param str zap_trace_span: OpenTracing span context
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """  # noqa: E501
        local_var_params, path_params, query_params, header_params, body_params = \
            self._post_signout_prepare(**kwargs)  # noqa: E501

        return await self.api_client.call_api(
            '/api/v2/signout', 'POST',
            path_params,
            query_params,
            header_params,
            body=body_params,
            post_params=[],
            files={},
            response_type=None,  # noqa: E501
            auth_settings=[],
            async_req=local_var_params.get('async_req'),
            _return_http_data_only=local_var_params.get('_return_http_data_only'),  # noqa: E501
            _preload_content=local_var_params.get('_preload_content', True),
            _request_timeout=local_var_params.get('_request_timeout'),
            collection_formats={},
            urlopen_kw=kwargs.get('urlopen_kw', None))

    def _post_signout_prepare(self, **kwargs):  # noqa: E501,D401,D403
        local_var_params = locals()

        all_params = ['zap_trace_span']  # noqa: E501
        self._check_operation_params('post_signout', all_params, local_var_params)

        path_params = {}

        query_params = []

        header_params = {}
        if 'zap_trace_span' in local_var_params:
            header_params['Zap-Trace-Span'] = local_var_params['zap_trace_span']  # noqa: E501

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.select_header_accept(
            ['application/json'])  # noqa: E501

        return local_var_params, path_params, query_params, header_params, body_params
