"""Commons function for Sync and Async client."""
from __future__ import absolute_import

import base64
import configparser
import logging
import os
from datetime import datetime, timedelta
from typing import List, Generator, Any, Union, Iterable, AsyncGenerator

from urllib3 import HTTPResponse

from influxdb_client_3.write_client.configuration import Configuration
from influxdb_client_3.write_client.service.write_service import WriteService

from influxdb_client_3.write_client.client.write.dataframe_serializer import DataframeSerializer
from influxdb_client_3.write_client.rest import _UTF_8_encoding

try:
    import dataclasses

    _HAS_DATACLASS = True
except ModuleNotFoundError:
    _HAS_DATACLASS = False

LOGGERS_NAMES = [
    'influxdb_client.client.influxdb_client',
    'influxdb_client.client.influxdb_client_async',
    'influxdb_client.client.write_api',
    'influxdb_client.client.write_api_async',
    'influxdb_client.client.write.retry',
    'influxdb_client.client.write.dataframe_serializer',
    'influxdb_client.client.util.multiprocessing_helper',
    'influxdb_client.client.http',
    'influxdb_client.client.exceptions',
]


# noinspection PyMethodMayBeStatic
class _BaseClient(object):
    def __init__(self, url, token, debug=None, timeout=10_000, enable_gzip=False, org: str = None,
                 default_tags: dict = None, http_client_logger: str = None, **kwargs) -> None:
        self.url = url
        self.token = token
        self.org = org

        self.default_tags = default_tags

        self.conf = _Configuration()
        if self.url.endswith("/"):
            self.conf.host = self.url[:-1]
        else:
            self.conf.host = self.url
        self.conf.enable_gzip = enable_gzip
        self.conf.verify_ssl = kwargs.get('verify_ssl', True)
        self.conf.ssl_ca_cert = kwargs.get('ssl_ca_cert', None)
        self.conf.cert_file = kwargs.get('cert_file', None)
        self.conf.cert_key_file = kwargs.get('cert_key_file', None)
        self.conf.cert_key_password = kwargs.get('cert_key_password', None)
        self.conf.ssl_context = kwargs.get('ssl_context', None)
        self.conf.proxy = kwargs.get('proxy', None)
        self.conf.proxy_headers = kwargs.get('proxy_headers', None)
        self.conf.connection_pool_maxsize = kwargs.get('connection_pool_maxsize', self.conf.connection_pool_maxsize)
        self.conf.timeout = timeout
        # logging
        self.conf.loggers["http_client_logger"] = logging.getLogger(http_client_logger)
        for client_logger in LOGGERS_NAMES:
            self.conf.loggers[client_logger] = logging.getLogger(client_logger)
        self.conf.debug = debug

        self.conf.username = kwargs.get('username', None)
        self.conf.password = kwargs.get('password', None)
        # defaults
        self.auth_header_name = None
        self.auth_header_value = None
        # by token
        if self.token:
            self.auth_header_name = "Authorization"
            self.auth_header_value = "Token " + self.token
        # by HTTP basic
        auth_basic = kwargs.get('auth_basic', False)
        if auth_basic:
            self.auth_header_name = "Authorization"
            self.auth_header_value = "Basic " + base64.b64encode(token.encode()).decode()
        # by username, password
        if self.conf.username and self.conf.password:
            self.auth_header_name = None
            self.auth_header_value = None

        self.retries = kwargs.get('retries', False)

        self.profilers = kwargs.get('profilers', None)
        pass

    @classmethod
    def _from_config_file(cls, config_file: str = "config.ini", debug=None, enable_gzip=False, **kwargs):
        config = configparser.ConfigParser()
        config_name = kwargs.get('config_name', 'influx2')
        is_json = False
        try:
            config.read(config_file)
        except configparser.ParsingError:
            with open(config_file) as json_file:
                import json
                config = json.load(json_file)
                is_json = True

        def _config_value(key: str):
            value = str(config[key]) if is_json else config[config_name][key]
            return value.strip('"')

        def _has_option(key: str):
            return key in config if is_json else config.has_option(config_name, key)

        def _has_section(key: str):
            return key in config if is_json else config.has_section(key)

        url = _config_value('url')
        token = _config_value('token')

        timeout = None
        if _has_option('timeout'):
            timeout = _config_value('timeout')

        org = None
        if _has_option('org'):
            org = _config_value('org')

        verify_ssl = True
        if _has_option('verify_ssl'):
            verify_ssl = _config_value('verify_ssl')

        ssl_ca_cert = None
        if _has_option('ssl_ca_cert'):
            ssl_ca_cert = _config_value('ssl_ca_cert')

        cert_file = None
        if _has_option('cert_file'):
            cert_file = _config_value('cert_file')

        cert_key_file = None
        if _has_option('cert_key_file'):
            cert_key_file = _config_value('cert_key_file')

        cert_key_password = None
        if _has_option('cert_key_password'):
            cert_key_password = _config_value('cert_key_password')

        connection_pool_maxsize = None
        if _has_option('connection_pool_maxsize'):
            connection_pool_maxsize = _config_value('connection_pool_maxsize')

        auth_basic = False
        if _has_option('auth_basic'):
            auth_basic = _config_value('auth_basic')

        default_tags = None
        if _has_section('tags'):
            if is_json:
                default_tags = config['tags']
            else:
                tags = {k: v.strip('"') for k, v in config.items('tags')}
                default_tags = dict(tags)

        profilers = None
        if _has_option('profilers'):
            profilers = [x.strip() for x in _config_value('profilers').split(',')]

        proxy = None
        if _has_option('proxy'):
            proxy = _config_value('proxy')

        return cls(url, token, debug=debug, timeout=_to_int(timeout), org=org, default_tags=default_tags,
                   enable_gzip=enable_gzip, verify_ssl=_to_bool(verify_ssl), ssl_ca_cert=ssl_ca_cert,
                   cert_file=cert_file, cert_key_file=cert_key_file, cert_key_password=cert_key_password,
                   connection_pool_maxsize=_to_int(connection_pool_maxsize), auth_basic=_to_bool(auth_basic),
                   profilers=profilers, proxy=proxy, **kwargs)

    @classmethod
    def _from_env_properties(cls, debug=None, enable_gzip=False, **kwargs):
        url = os.getenv('INFLUXDB_V2_URL', "http://localhost:8086")
        token = os.getenv('INFLUXDB_V2_TOKEN', "my-token")
        timeout = os.getenv('INFLUXDB_V2_TIMEOUT', "10000")
        org = os.getenv('INFLUXDB_V2_ORG', "my-org")
        verify_ssl = os.getenv('INFLUXDB_V2_VERIFY_SSL', "True")
        ssl_ca_cert = os.getenv('INFLUXDB_V2_SSL_CA_CERT', None)
        cert_file = os.getenv('INFLUXDB_V2_CERT_FILE', None)
        cert_key_file = os.getenv('INFLUXDB_V2_CERT_KEY_FILE', None)
        cert_key_password = os.getenv('INFLUXDB_V2_CERT_KEY_PASSWORD', None)
        connection_pool_maxsize = os.getenv('INFLUXDB_V2_CONNECTION_POOL_MAXSIZE', None)
        auth_basic = os.getenv('INFLUXDB_V2_AUTH_BASIC', "False")

        prof = os.getenv("INFLUXDB_V2_PROFILERS", None)
        profilers = None
        if prof is not None:
            profilers = [x.strip() for x in prof.split(',')]

        default_tags = dict()

        for key, value in os.environ.items():
            if key.startswith("INFLUXDB_V2_TAG_"):
                default_tags[key[16:].lower()] = value

        return cls(url, token, debug=debug, timeout=_to_int(timeout), org=org, default_tags=default_tags,
                   enable_gzip=enable_gzip, verify_ssl=_to_bool(verify_ssl), ssl_ca_cert=ssl_ca_cert,
                   cert_file=cert_file, cert_key_file=cert_key_file, cert_key_password=cert_key_password,
                   connection_pool_maxsize=_to_int(connection_pool_maxsize), auth_basic=_to_bool(auth_basic),
                   profilers=profilers, **kwargs)


  
class _BaseWriteApi(object):
    def __init__(self, influxdb_client, point_settings=None):
        self._influxdb_client = influxdb_client
        self._point_settings = point_settings
        self._write_service = WriteService(influxdb_client.api_client)
        if influxdb_client.default_tags:
            for key, value in influxdb_client.default_tags.items():
                self._point_settings.add_default_tag(key, value)

    def _append_default_tag(self, key, val, record):
        from write_client import Point
        if isinstance(record, bytes) or isinstance(record, str):
            pass
        elif isinstance(record, Point):
            record.tag(key, val)
        elif isinstance(record, dict):
            record.setdefault("tags", {})
            record.get("tags")[key] = val
        elif isinstance(record, Iterable):
            for item in record:
                self._append_default_tag(key, val, item)

    def _append_default_tags(self, record):
        if self._point_settings.defaultTags and record is not None:
            for key, val in self._point_settings.defaultTags.items():
                self._append_default_tag(key, val, record)

    def _serialize(self, record, write_precision, payload, **kwargs):
        from influxdb_client_3.write_client.client.write.point import Point
        if isinstance(record, bytes):
            payload[write_precision].append(record)

        elif isinstance(record, str):
            self._serialize(record.encode(_UTF_8_encoding), write_precision, payload, **kwargs)

        elif isinstance(record, Point):
            precision_from_point = kwargs.get('precision_from_point', True)
            precision = record.write_precision if precision_from_point else write_precision
            self._serialize(record.to_line_protocol(precision=precision), precision, payload, **kwargs)

        elif isinstance(record, dict):
            self._serialize(Point.from_dict(record, write_precision=write_precision, **kwargs),
                            write_precision, payload, **kwargs)
        elif 'DataFrame' in type(record).__name__:
            serializer = DataframeSerializer(record, self._point_settings, write_precision, **kwargs)
            self._serialize(serializer.serialize(), write_precision, payload, **kwargs)
        elif hasattr(record, "_asdict"):
            # noinspection PyProtectedMember
            self._serialize(record._asdict(), write_precision, payload, **kwargs)
        elif _HAS_DATACLASS and dataclasses.is_dataclass(record):
            self._serialize(dataclasses.asdict(record), write_precision, payload, **kwargs)
        elif isinstance(record, Iterable):
            for item in record:
                self._serialize(item, write_precision, payload, **kwargs)



class _Configuration(Configuration):
    def __init__(self):
        Configuration.__init__(self)
        self.enable_gzip = False
        self.username = None
        self.password = None

    def update_request_header_params(self, path: str, params: dict):
        super().update_request_header_params(path, params)
        if self.enable_gzip:
            # GZIP Request
            if path == '/api/v2/write':
                params["Content-Encoding"] = "gzip"
                params["Accept-Encoding"] = "identity"
                pass
            # GZIP Response
            if path == '/api/v2/query':
                # params["Content-Encoding"] = "gzip"
                params["Accept-Encoding"] = "gzip"
                pass
            pass
        pass

    def update_request_body(self, path: str, body):
        _body = super().update_request_body(path, body)
        if self.enable_gzip:
            # GZIP Request
            if path == '/api/v2/write':
                import gzip
                if isinstance(_body, bytes):
                    return gzip.compress(data=_body)
                else:
                    return gzip.compress(bytes(_body, _UTF_8_encoding))

        return _body


def _to_bool(bool_value):
    return str(bool_value).lower() in ("yes", "true")


def _to_int(int_value):
    return int(int_value) if int_value is not None else None
