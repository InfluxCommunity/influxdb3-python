# coding: utf-8

from __future__ import absolute_import

import copy
import logging
import multiprocessing
import sys

import urllib3


class TypeWithDefault(type):

    def __init__(cls, name, bases, dct):
        """Initialize with defaults."""
        super(TypeWithDefault, cls).__init__(name, bases, dct)
        cls._default = None

    def __call__(cls):
        """Call self as a function."""
        if cls._default is None:
            cls._default = type.__call__(cls)
        return copy.copy(cls._default)

    def set_default(cls, default):
        """Set dafaults."""
        cls._default = copy.copy(default)


class Configuration(object, metaclass=TypeWithDefault):

    def __init__(self):
        """Initialize configuration."""
        # Default Base url
        self.host = "http://localhost/api/v2"
        # Temp file folder for downloading files
        self.temp_folder_path = None

        # Authentication Settings
        # dict to store API key(s)
        self.api_key = {}
        # dict to store API prefix (e.g. Bearer)
        self.api_key_prefix = {}
        # Username for HTTP basic authentication
        self.username = ""
        # Password for HTTP basic authentication
        self.password = ""

        # Logging Settings
        self.loggers = {}
        # Log format
        self.logger_format = '%(asctime)s %(levelname)s %(message)s'
        # Log stream handler
        self.logger_stream_handler = None
        # Log file handler
        self.logger_file_handler = None
        # Debug file location
        self.logger_file = None
        # Debug switch
        self.debug = False

        # SSL/TLS verification
        # Set this to false to skip verifying SSL certificate when calling API
        # from https server.
        self.verify_ssl = True
        # Set this to customize the certificate file to verify the peer.
        self.ssl_ca_cert = None
        # client certificate file
        self.cert_file = None
        # client key file
        self.cert_key_file = None
        # client key file password
        self.cert_key_password = None
        # Set this to True/False to enable/disable SSL hostname verification.
        self.assert_hostname = None

        # Set this to specify a custom ssl context to inject this context inside the urllib3 connection pool.
        self.ssl_context = None

        # urllib3 connection pool's maximum number of connections saved
        # per pool. urllib3 uses 1 connection as default value, but this is
        # not the best value when you are making a lot of possibly parallel
        # requests to the same host, which is often the case here.
        # cpu_count * 5 is used as default value to increase performance.
        self.connection_pool_maxsize = multiprocessing.cpu_count() * 5
        # Timeout setting for a request. If one number provided, it will be total request timeout.
        # It can also be a pair (tuple) of (connection, read) timeouts.
        self.timeout = None

        # Set to True/False to enable basic authentication when using proxied InfluxDB 1.8.x with no auth-enabled
        self.auth_basic = False

        # Proxy URL
        self.proxy = None
        # A dictionary containing headers that will be sent to the proxy
        self.proxy_headers = None
        # Safe chars for path_param
        self.safe_chars_for_path_param = ''

        # Compression settings
        self.enable_gzip = False
        self.gzip_threshold = None

    @property
    def logger_file(self):
        """Logger file.

        If the logger_file is None, then add stream handler and remove file
        handler. Otherwise, add file handler and remove stream handler.

        :param value: The logger_file path.
        :type: str
        """
        return self.__logger_file

    @logger_file.setter
    def logger_file(self, value):
        """Logger file.

        If the logger_file is None, then add stream handler and remove file
        handler. Otherwise, add file handler and remove stream handler.

        :param value: The logger_file path.
        :type: str
        """
        self.__logger_file = value
        if self.__logger_file:
            # If set logging file,
            # then add file handler and remove stream handler.
            self.logger_file_handler = logging.FileHandler(self.__logger_file)
            self.logger_file_handler.setFormatter(self.logger_formatter)
            for _, logger in self.loggers.items():
                logger.addHandler(self.logger_file_handler)

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
            for name, logger in self.loggers.items():
                logger.setLevel(logging.DEBUG)
                if name == 'influxdb_client.client.http':
                    # makes sure to do not duplicate stdout handler
                    if not any(map(lambda h: isinstance(h, logging.StreamHandler) and h.stream == sys.stdout,
                                   logger.handlers)):
                        logger.addHandler(logging.StreamHandler(sys.stdout))
            # we use 'influxdb_client.client.http' logger instead of this
            # httplib.HTTPConnection.debuglevel = 1
        else:
            # if debug status is False, turn off debug logging,
            # setting log level to default `logging.WARNING`
            for _, logger in self.loggers.items():
                logger.setLevel(logging.WARNING)
            # we use 'influxdb_client.client.http' logger instead of this
            # httplib.HTTPConnection.debuglevel = 0

    @property
    def logger_format(self):
        """Logger format.

        The logger_formatter will be updated when sets logger_format.

        :param value: The format string.
        :type: str
        """
        return self.__logger_format

    @logger_format.setter
    def logger_format(self, value):
        """Logger format.

        The logger_formatter will be updated when sets logger_format.

        :param value: The format string.
        :type: str
        """
        self.__logger_format = value
        self.logger_formatter = logging.Formatter(self.__logger_format)

    def get_api_key_with_prefix(self, identifier):
        """Get API key (with prefix if set).

        :param identifier: The identifier of apiKey.
        :return: The token for api key authentication.
        """
        if (self.api_key.get(identifier) and
                self.api_key_prefix.get(identifier)):
            return self.api_key_prefix[identifier] + ' ' + self.api_key[identifier]  # noqa: E501
        elif self.api_key.get(identifier):
            return self.api_key[identifier]

    def get_basic_auth_token(self):
        """Get HTTP basic authentication header (string).

        :return: The token for basic HTTP authentication.
        """
        return urllib3.util.make_headers(
            basic_auth=self.username + ':' + self.password
        ).get('authorization')

    def auth_settings(self):
        """Get Auth Settings dict for api client.

        :return: The Auth Settings information dict.
        """
        return {
            'BasicAuthentication':
                {
                    'type': 'basic',
                    'in': 'header',
                    'key': 'Authorization',
                    'value': self.get_basic_auth_token()
                },
            'TokenAuthentication':
                {
                    'type': 'api_key',
                    'in': 'header',
                    'key': 'Authorization',
                    'value': self.get_api_key_with_prefix('Authorization')
                },

        }

    def to_debug_report(self):
        """Get the essential information for debugging.

        :return: The report for debugging.
        """
        from write_client import VERSION
        return "Python SDK Debug Report:\n"\
               "OS: {env}\n"\
               "Python Version: {pyversion}\n"\
               "Version of the API: 2.0.0\n"\
               "SDK Package Version: {client_version}".\
               format(env=sys.platform, pyversion=sys.version, client_version=VERSION)

    def update_request_header_params(self, path: str, params: dict, should_gzip: bool = False):
        """Update header params based on custom settings.

        :param path: Resource path.
        :param params: Header parameters dict to be updated.
        :param should_gzip: Describes if request body should be gzip compressed.
        """
        pass

    def update_request_body(self, path: str, body, should_gzip: bool = False):
        """Update http body based on custom settings.

        :param path: Resource path.
        :param body: Request body to be updated.
        :param should_gzip: Describes if request body should be gzip compressed.
        :return: Updated body
        """
        return body
