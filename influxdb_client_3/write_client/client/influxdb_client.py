"""InfluxDBClient is client for API defined in https://github.com/influxdata/influxdb/blob/master/http/swagger.yml."""

from __future__ import absolute_import

import logging
import warnings


from influxdb_client_3.write_client.client._base import _BaseClient
from influxdb_client_3.write_client.client.write_api import WriteApi, WriteOptions, PointSettings

logger = logging.getLogger('influxdb_client_3.write_client.client.influxdb_client')


class InfluxDBClient(_BaseClient):
    """InfluxDBClient is client for InfluxDB v2."""

    def __init__(self, url, token: str = None, debug=None, timeout=10_000, enable_gzip=False, org: str = None,
                 default_tags: dict = None, **kwargs) -> None:
        """
        Initialize defaults.

        :param url: InfluxDB server API url (ex. http://localhost:8086).
        :param token: ``token`` to authenticate to the InfluxDB API
        :param debug: enable verbose logging of http requests
        :param timeout: HTTP client timeout setting for a request specified in milliseconds.
                        If one number provided, it will be total request timeout.
                        It can also be a pair (tuple) of (connection, read) timeouts.
        :param enable_gzip: Enable Gzip compression for http requests. Currently, only the "Write" and "Query" endpoints
                            supports the Gzip compression.
        :param org: organization name (used as a default in Query, Write and Delete API)
        :key bool verify_ssl: Set this to false to skip verifying SSL certificate when calling API from https server.
        :key str ssl_ca_cert: Set this to customize the certificate file to verify the peer.
        :key str cert_file: Path to the certificate that will be used for mTLS authentication.
        :key str cert_key_file: Path to the file contains private key for mTLS certificate.
        :key str cert_key_password: String or function which returns password for decrypting the mTLS private key.
        :key ssl.SSLContext ssl_context: Specify a custom Python SSL Context for the TLS/ mTLS handshake.
                                         Be aware that only delivered certificate/ key files or an SSL Context are
                                         possible.
        :key str proxy: Set this to configure the http proxy to be used (ex. http://localhost:3128)
        :key str proxy_headers: A dictionary containing headers that will be sent to the proxy. Could be used for proxy
                                authentication.
        :key int connection_pool_maxsize: Number of connections to save that can be reused by urllib3.
                                          Defaults to "multiprocessing.cpu_count() * 5".
        :key urllib3.util.retry.Retry retries: Set the default retry strategy that is used for all HTTP requests
                                               except batching writes. As a default there is no one retry strategy.
        :key bool auth_basic: Set this to true to enable basic authentication when talking to a InfluxDB 1.8.x that
                              does not use auth-enabled but is protected by a reverse proxy with basic authentication.
                              (defaults to false, don't set to true when talking to InfluxDB 2)
        :key str username: ``username`` to authenticate via username and password credentials to the InfluxDB 2.x
        :key str password: ``password`` to authenticate via username and password credentials to the InfluxDB 2.x
        :key list[str] profilers: list of enabled Flux profilers
        """
        super().__init__(url=url, token=token, debug=debug, timeout=timeout, enable_gzip=enable_gzip, org=org,
                         default_tags=default_tags, http_client_logger="urllib3", **kwargs)

        from influxdb_client_3.write_client._sync.api_client import ApiClient
        self.api_client = ApiClient(configuration=self.conf, header_name=self.auth_header_name,
                                    header_value=self.auth_header_value, retries=self.retries)

    def __enter__(self):
        """
        Enter the runtime context related to this object.

        It will bind this method’s return value to the target(s)
        specified in the `as` clause of the statement.

        return: self instance
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object and close the client."""
        self.close()

    @classmethod
    def from_config_file(cls, config_file: str = "config.ini", debug=None, enable_gzip=False, **kwargs):
        """
        Configure client via configuration file. The configuration has to be under 'influx' section.

        :param config_file: Path to configuration file
        :param debug: Enable verbose logging of http requests
        :param enable_gzip: Enable Gzip compression for http requests. Currently, only the "Write" and "Query" endpoints
                            supports the Gzip compression.
        :key config_name: Name of the configuration section of the configuration file
        :key str proxy_headers: A dictionary containing headers that will be sent to the proxy. Could be used for proxy
                                authentication.
        :key urllib3.util.retry.Retry retries: Set the default retry strategy that is used for all HTTP requests
                                               except batching writes. As a default there is no one retry strategy.
        :key ssl.SSLContext ssl_context: Specify a custom Python SSL Context for the TLS/ mTLS handshake.
                                         Be aware that only delivered certificate/ key files or an SSL Context are
                                         possible.

        The supported formats:
            - https://docs.python.org/3/library/configparser.html
            - https://toml.io/en/
            - https://www.json.org/json-en.html

        Configuration options:
            - url
            - org
            - token
            - timeout,
            - verify_ssl
            - ssl_ca_cert
            - cert_file
            - cert_key_file
            - cert_key_password
            - connection_pool_maxsize
            - auth_basic
            - profilers
            - proxy


        config.ini example::

            [influx2]
            url=http://localhost:8086
            org=my-org
            token=my-token
            timeout=6000
            connection_pool_maxsize=25
            auth_basic=false
            profilers=query,operator
            proxy=http:proxy.domain.org:8080

            [tags]
            id = 132-987-655
            customer = California Miner
            data_center = ${env.data_center}

        config.toml example::

            [influx2]
                url = "http://localhost:8086"
                token = "my-token"
                org = "my-org"
                timeout = 6000
                connection_pool_maxsize = 25
                auth_basic = false
                profilers="query, operator"
                proxy = "http://proxy.domain.org:8080"

            [tags]
                id = "132-987-655"
                customer = "California Miner"
                data_center = "${env.data_center}"

        config.json example::

            {
                "url": "http://localhost:8086",
                "token": "my-token",
                "org": "my-org",
                "active": true,
                "timeout": 6000,
                "connection_pool_maxsize": 55,
                "auth_basic": false,
                "profilers": "query, operator",
                "tags": {
                    "id": "132-987-655",
                    "customer": "California Miner",
                    "data_center": "${env.data_center}"
                }
            }

        """
        return InfluxDBClient._from_config_file(config_file=config_file, debug=debug, enable_gzip=enable_gzip, **kwargs)

    @classmethod
    def from_env_properties(cls, debug=None, enable_gzip=False, **kwargs):
        """
        Configure client via environment properties.

        :param debug: Enable verbose logging of http requests
        :param enable_gzip: Enable Gzip compression for http requests. Currently, only the "Write" and "Query" endpoints
                            supports the Gzip compression.
        :key str proxy: Set this to configure the http proxy to be used (ex. http://localhost:3128)
        :key str proxy_headers: A dictionary containing headers that will be sent to the proxy. Could be used for proxy
                                authentication.
        :key urllib3.util.retry.Retry retries: Set the default retry strategy that is used for all HTTP requests
                                               except batching writes. As a default there is no one retry strategy.
        :key ssl.SSLContext ssl_context: Specify a custom Python SSL Context for the TLS/ mTLS handshake.
                                         Be aware that only delivered certificate/ key files or an SSL Context are
                                         possible.

        Supported environment properties:
            - INFLUXDB_V2_URL
            - INFLUXDB_V2_ORG
            - INFLUXDB_V2_TOKEN
            - INFLUXDB_V2_TIMEOUT
            - INFLUXDB_V2_VERIFY_SSL
            - INFLUXDB_V2_SSL_CA_CERT
            - INFLUXDB_V2_CERT_FILE
            - INFLUXDB_V2_CERT_KEY_FILE
            - INFLUXDB_V2_CERT_KEY_PASSWORD
            - INFLUXDB_V2_CONNECTION_POOL_MAXSIZE
            - INFLUXDB_V2_AUTH_BASIC
            - INFLUXDB_V2_PROFILERS
            - INFLUXDB_V2_TAG
        """
        return InfluxDBClient._from_env_properties(debug=debug, enable_gzip=enable_gzip, **kwargs)

    def write_api(self, write_options=WriteOptions(), point_settings=PointSettings(), **kwargs) -> WriteApi:
        """
        Create Write API instance.

        Example:
            .. code-block:: python

                from influxdb_client import InfluxDBClient
                from influxdb_client.client.write_api import SYNCHRONOUS


                # Initialize SYNCHRONOUS instance of WriteApi
                with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
                    write_api = client.write_api(write_options=SYNCHRONOUS)

        If you would like to use a **background batching**, you have to configure client like this:

        .. code-block:: python

            from influxdb_client import InfluxDBClient

            # Initialize background batching instance of WriteApi
            with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
                with client.write_api() as write_api:
                    pass

        There is also possibility to use callbacks to notify about state of background batches:

        .. code-block:: python

            from influxdb_client import InfluxDBClient
            from influxdb_client.client.exceptions import InfluxDBError


            class BatchingCallback(object):

                def success(self, conf: (str, str, str), data: str):
                    print(f"Written batch: {conf}, data: {data}")

                def error(self, conf: (str, str, str), data: str, exception: InfluxDBError):
                    print(f"Cannot write batch: {conf}, data: {data} due: {exception}")

                def retry(self, conf: (str, str, str), data: str, exception: InfluxDBError):
                    print(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")


            with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
                callback = BatchingCallback()
                with client.write_api(success_callback=callback.success,
                                      error_callback=callback.error,
                                      retry_callback=callback.retry) as write_api:
                    pass

        :param write_options: Write API configuration
        :param point_settings: settings to store default tags
        :key success_callback: The callable ``callback`` to run after successfully writen a batch.

                               The callable must accept two arguments:
                                    - `Tuple`: ``(bucket, organization, precision)``
                                    - `str`: written data

                               **[batching mode]**

        :key error_callback: The callable ``callback`` to run after unsuccessfully writen a batch.

                             The callable must accept three arguments:
                                - `Tuple`: ``(bucket, organization, precision)``
                                - `str`: written data
                                - `Exception`: an occurred error

                             **[batching mode]**
        :key retry_callback: The callable ``callback`` to run after retryable error occurred.

                             The callable must accept three arguments:
                                - `Tuple`: ``(bucket, organization, precision)``
                                - `str`: written data
                                - `Exception`: an retryable error

                             **[batching mode]**
        :return: write api instance
        """
        return WriteApi(influxdb_client=self, write_options=write_options, point_settings=point_settings, **kwargs)


    def close(self):
        """Shutdown the client."""
        self.__del__()

    def __del__(self):
        """Shutdown the client."""
        if self.api_client:
            self.api_client.__del__()
            self.api_client = None





