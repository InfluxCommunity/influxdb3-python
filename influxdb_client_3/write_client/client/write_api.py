"""Collect and write time series data to InfluxDB Cloud or InfluxDB OSS."""

# coding: utf-8
# TODO Remove after this program no longer supports Python 3.8.*
from __future__ import annotations

import datetime
import logging
import os
import warnings
from collections import defaultdict
from enum import Enum
from http import HTTPStatus
from multiprocessing.pool import ThreadPool
from random import random
from time import sleep
from typing import Union, Any, Iterable, NamedTuple

import reactivex as rx
import urllib3
from reactivex import operators as ops, Observable
from reactivex.scheduler import ThreadPoolScheduler
from reactivex.subject import Subject

from influxdb_client_3.exceptions import InfluxDBPartialWriteError
from influxdb_client_3.write_client._sync.rest_client import RestClient
from influxdb_client_3.write_client.client._base import _HAS_DATACLASS
from influxdb_client_3.write_client.client.write.dataframe_serializer import DataframeSerializer
from influxdb_client_3.write_client.client.write.point import Point, DEFAULT_WRITE_PRECISION, sanitize_tag_order
from influxdb_client_3.write_client.client.write.retry import WritesRetry
from influxdb_client_3.write_client.domain import WritePrecision
from influxdb_client_3.write_client.domain.write_precision_converter import WritePrecisionConverter
from influxdb_client_3.write_client.rest import _UTF_8_encoding, ApiException
from influxdb_client_3.write_client.write_defaults import (
    DEFAULT_WRITE_ACCEPT_PARTIAL as _DEFAULT_WRITE_ACCEPT_PARTIAL,
    DEFAULT_WRITE_NO_SYNC as _DEFAULT_WRITE_NO_SYNC,
    DEFAULT_WRITE_TIMEOUT as _DEFAULT_WRITE_TIMEOUT,
    DEFAULT_WRITE_USE_V2_API as _DEFAULT_WRITE_USE_V2_API,
)

# Deprecated compatibility aliases.
# New code should import these defaults from `influxdb_client_3.write_client.write_defaults`.
DEFAULT_WRITE_NO_SYNC = _DEFAULT_WRITE_NO_SYNC
DEFAULT_WRITE_TIMEOUT = _DEFAULT_WRITE_TIMEOUT
DEFAULT_WRITE_ACCEPT_PARTIAL = _DEFAULT_WRITE_ACCEPT_PARTIAL
DEFAULT_WRITE_USE_V2_API = _DEFAULT_WRITE_USE_V2_API

# Kwargs consumed during serialization that should not be passed to _post_write
SERIALIZER_KWARGS = {
    # DataFrame-specific kwargs
    'data_frame_measurement_name',
    'data_frame_tag_columns',
    'data_frame_timestamp_column',
    'data_frame_timestamp_timezone',
    # Record-specific kwargs (dict, NamedTuple, dataclass)
    'record_measurement_key',
    'record_measurement_name',
    'record_time_key',
    'record_tag_keys',
    'record_field_keys',
    # Point serialization-specific kwargs
    'tag_order',
}

logger = logging.getLogger('influxdb_client_3.write_client.client.write_api')

if _HAS_DATACLASS:
    import dataclasses
    from dataclasses import dataclass


class WriteType(Enum):
    """Configuration which type of writes will client use."""

    batching = 1
    asynchronous = 2
    synchronous = 3


class DefaultWriteOptions(Enum):
    write_type = WriteType.synchronous
    write_precision = DEFAULT_WRITE_PRECISION
    no_sync = DEFAULT_WRITE_NO_SYNC
    accept_partial = DEFAULT_WRITE_ACCEPT_PARTIAL
    use_v2_api = DEFAULT_WRITE_USE_V2_API
    timeout = DEFAULT_WRITE_TIMEOUT


class WriteOptions(object):
    """Write configuration."""

    def __init__(self, write_type: WriteType = WriteType.batching,
                 batch_size=1_000, flush_interval=1_000,
                 jitter_interval=0,
                 retry_interval=5_000,
                 max_retries=5,
                 max_retry_delay=125_000,
                 max_retry_time=180_000,
                 exponential_base=2,
                 max_close_wait=300_000,
                 write_precision=DEFAULT_WRITE_PRECISION,
                 no_sync=DEFAULT_WRITE_NO_SYNC,
                 tag_order=None,
                 accept_partial=DEFAULT_WRITE_ACCEPT_PARTIAL,
                 use_v2_api=DEFAULT_WRITE_USE_V2_API,
                 timeout=DEFAULT_WRITE_TIMEOUT,
                 write_scheduler=ThreadPoolScheduler(max_workers=1)) -> None:
        """
        Create write api configuration.

        :param write_type: methods of write (batching, asynchronous, synchronous)
        :param batch_size: the number of data point to collect in batch
        :param flush_interval: flush data at least in this interval (milliseconds)
        :param jitter_interval: this is primarily to avoid large write spikes for users running a large number of
               client instances ie, a jitter of 5s and flush duration 10s means flushes will happen every 10-15s
               (milliseconds)
        :param retry_interval: the time to wait before retry unsuccessful write (milliseconds)
        :param max_retries: the number of max retries when write fails, 0 means retry is disabled
        :param max_retry_delay: the maximum delay between each retry attempt in milliseconds
        :param max_retry_time: total timeout for all retry attempts in milliseconds, if 0 retry is disabled
        :param exponential_base: base for the exponential retry delay
        :param max_close_wait: the maximum time to wait for writes to be flushed if close() is called
        :param write_precision: precision to use when writing points to InfluxDB
        :param no_sync: skip waiting for WAL persistence on write
        :param accept_partial: allow partial writes when some lines fail
        :param tag_order: optional list of tag names used to prioritize tag serialization order
        :param use_v2_api: use /api/v2/write compatibility endpoint
        :param timeout: timeout to use when writing to the database in milliseconds. Default is 10_000
        :param write_scheduler:
        """
        self.write_type = write_type
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.jitter_interval = jitter_interval
        self.retry_interval = retry_interval
        self.max_retries = max_retries
        self.max_retry_delay = max_retry_delay
        self.max_retry_time = max_retry_time
        self.exponential_base = exponential_base
        self.write_scheduler = write_scheduler
        self.max_close_wait = max_close_wait
        self.write_precision = write_precision
        self.timeout = timeout
        self.no_sync = no_sync
        self.accept_partial = accept_partial
        self.use_v2_api = use_v2_api
        self.tag_order = sanitize_tag_order(tag_order)

    def validate(self):
        if self.use_v2_api and self.no_sync:
            raise ValueError("invalid write options: no_sync cannot be used with use_v2_api")

    def to_retry_strategy(self, **kwargs):
        """
        Create a Retry strategy from write options.

        :key retry_callback: The callable ``callback`` to run after retryable error occurred.
                             The callable must accept one argument:
                                - `Exception`: an retryable error
        """
        return WritesRetry(
            total=self.max_retries,
            retry_interval=self.retry_interval / 1_000,
            jitter_interval=self.jitter_interval / 1_000,
            max_retry_delay=self.max_retry_delay / 1_000,
            max_retry_time=self.max_retry_time / 1_000,
            exponential_base=self.exponential_base,
            retry_callback=kwargs.get("retry_callback", None),
            allowed_methods=["POST"])

    def __getstate__(self):
        """Return a dict of attributes that you want to pickle."""
        state = self.__dict__.copy()
        # Remove write scheduler
        del state['write_scheduler']
        return state

    def __setstate__(self, state):
        """Set your object with the provided dict."""
        self.__dict__.update(state)
        # Init default write Scheduler
        self.write_scheduler = ThreadPoolScheduler(max_workers=1)


SYNCHRONOUS = WriteOptions(write_type=WriteType.synchronous)
ASYNCHRONOUS = WriteOptions(write_type=WriteType.asynchronous)


class PointSettings(object):
    """Settings to store default tags."""

    def __init__(self, **default_tags) -> None:
        """
        Create point settings for write api.

        :param default_tags: Default tags which will be added to each point written by api.
        """
        self.defaultTags = dict()

        for key, val in default_tags.items():
            self.add_default_tag(key, val)

    @staticmethod
    def _get_value(value):

        if value.startswith("${env."):
            return os.environ.get(value[6:-1])

        return value

    def add_default_tag(self, key, value) -> None:
        """Add new default tag with key and value."""
        self.defaultTags[key] = self._get_value(value)


class _BatchItemKey(object):
    def __init__(self, bucket, org, precision=DEFAULT_WRITE_PRECISION, **kwargs) -> None:
        self.bucket = bucket
        self.org = org
        self.precision = precision
        self.no_sync = kwargs.get('no_sync', DEFAULT_WRITE_NO_SYNC)
        self.accept_partial = kwargs.get('accept_partial', DEFAULT_WRITE_ACCEPT_PARTIAL)
        self.use_v2_api = kwargs.get('use_v2_api', DEFAULT_WRITE_USE_V2_API)
        self.kwargs = kwargs
        pass

    def __hash__(self) -> int:
        return hash((self.bucket, self.org, self.precision, self.no_sync, self.accept_partial, self.use_v2_api))

    def __eq__(self, o: object) -> bool:
        return isinstance(o, self.__class__) \
            and self.bucket == o.bucket \
            and self.org == o.org \
            and self.precision == o.precision \
            and self.no_sync == o.no_sync \
            and self.accept_partial == o.accept_partial \
            and self.use_v2_api == o.use_v2_api

    def __str__(self) -> str:
        return '_BatchItemKey[bucket:\'{}\', org:\'{}\', precision:\'{}\', kwargs: \'{}\']' \
            .format(str(self.bucket), str(self.org), str(self.precision), str(self.kwargs))


class _BatchItem(object):
    def __init__(self, key: _BatchItemKey, data, size=1) -> None:
        self.key = key
        self.data = data
        self.size = size
        pass

    def to_key_tuple(self) -> (str, str, str):
        return self.key.bucket, self.key.org, self.key.precision

    def __str__(self) -> str:
        return '_BatchItem[key:\'{}\', size: \'{}\']' \
            .format(str(self.key), str(self.size))


class _BatchResponse(object):
    def __init__(self, data: _BatchItem, exception: Exception = None):
        self.data = data
        self.exception = exception
        pass

    def __str__(self) -> str:
        return '_BatchResponse[status:\'{}\', \'{}\']' \
            .format("failed" if self.exception else "success", str(self.data))


def _body_reduce(batch_items):
    return b'\n'.join(map(lambda batch_item: batch_item.data, batch_items))


class WriteApi:
    PRIMITIVE_TYPES = (float, bool, bytes, str, int)
    _pool = None

    """
    Implementation for '/api/v2/write' and '/api/v3/write_lp' endpoint.

    Example:
        .. code-block:: python

            from influxdb_client import InfluxDBClient
            from influxdb_client.client.write_api import SYNCHRONOUS


            # Initialize SYNCHRONOUS instance of WriteApi
            with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
                write_api = client.write_api(write_options=SYNCHRONOUS)
    """

    def __init__(self,
                 token: str,
                 bucket: str,
                 org: str,
                 gzip_threshold=None,
                 enable_gzip=False,
                 auth_scheme=None,
                 timeout=None,
                 pool_threads=None,
                 default_header=None,
                 rest_client: RestClient = None,
                 write_options: WriteOptions = WriteOptions(),
                 point_settings: PointSettings = PointSettings(),
                 **kwargs) -> None:
        """
        Initialize defaults.

        :param influxdb_client: with default settings (organization)
        :param write_options: write api configuration
        :param point_settings: settings to store default tags.
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
        """
        self.rest_client = rest_client
        self.token = token
        self.bucket = bucket
        self.org = org
        self.enable_gzip = enable_gzip
        self.gzip_threshold = gzip_threshold
        self.auth_scheme = auth_scheme
        self.timeout = timeout
        self.pool_threads = pool_threads
        self._point_settings = point_settings
        self.default_header = default_header

        self._write_options = write_options
        # TODO - callbacks seem to be used with batching type only - could they be used with sync or async?
        self._success_callback = kwargs.get('success_callback', None)
        self._error_callback = kwargs.get('error_callback', None)
        self._retry_callback = kwargs.get('retry_callback', None)

        if self._write_options.write_type is WriteType.batching:
            self._subject, self._disposable = self._create_batching_pipeline()
        else:
            self._subject, self._disposable = None, None

        if self._write_options.write_type is WriteType.asynchronous:
            message = """The 'WriteType.asynchronous' is deprecated and will be removed in future major version.
            You can use native asynchronous version of the client:
        """
            # TODO above message has link to Influxdb2 API __NOT__ Influxdb3 API !!! - illustrates different API
            warnings.warn(message, DeprecationWarning)

    def write(self,
              bucket=None,
              org=None,
              record: Union[
                  str, Iterable['str'], Point, Iterable['Point'], dict, Iterable['dict'], bytes, Iterable['bytes'],
                  Observable, NamedTuple, Iterable['NamedTuple'], 'dataclass', Iterable['dataclass']
              ] = None,
              write_precision: WritePrecision = None,
              **kwargs) -> Any:
        """
        Write time-series data into InfluxDB.

        :param str bucket: specifies the destination bucket for writes (required)
        :param str, Organization org: specifies the destination organization for writes;
                                      take the ID, Name or Organization.
                                      If not specified the default value from ``InfluxDBClient.org`` is used.
        :param WritePrecision write_precision: specifies the precision for the unix timestamps within
                                               the body line-protocol. The precision specified on a Point has precedes
                                               and is use for write.
        :param record: Point, Line Protocol, Dictionary, NamedTuple, Data Classes, Pandas DataFrame or
                       RxPY Observable to write
        :key data_frame_measurement_name: name of measurement for writing Pandas DataFrame - ``DataFrame``
        :key data_frame_tag_columns: list of DataFrame columns which are tags,
                                     rest columns will be fields - ``DataFrame``
        :key data_frame_timestamp_column: name of DataFrame column which contains a timestamp. The column can be defined as a :class:`~str` value
                                          formatted as `2018-10-26`, `2018-10-26 12:00`, `2018-10-26 12:00:00-05:00`
                                          or other formats and types supported by `pandas.to_datetime <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html#pandas.to_datetime>`_ - ``DataFrame``
        :key data_frame_timestamp_timezone: name of the timezone which is used for timestamp column - ``DataFrame``
        :key record_measurement_key: key of record with specified measurement -
                                     ``dictionary``, ``NamedTuple``, ``dataclass``
        :key record_measurement_name: static measurement name - ``dictionary``, ``NamedTuple``, ``dataclass``
        :key record_time_key: key of record with specified timestamp - ``dictionary``, ``NamedTuple``, ``dataclass``
        :key record_tag_keys: list of record keys to use as a tag - ``dictionary``, ``NamedTuple``, ``dataclass``
        :key record_field_keys: list of record keys to use as a field  - ``dictionary``, ``NamedTuple``, ``dataclass``

        Example:
            .. code-block:: python

                # Record as Line Protocol
                write_api.write("my-bucket", "my-org", "h2o_feet,location=us-west level=125i 1")

                # Record as Dictionary
                dictionary = {
                    "measurement": "h2o_feet",
                    "tags": {"location": "us-west"},
                    "fields": {"level": 125},
                    "time": 1
                }
                write_api.write("my-bucket", "my-org", dictionary)

                # Record as Point
                from influxdb_client import Point
                point = Point("h2o_feet").tag("location", "us-west").field("level", 125).time(1)
                write_api.write("my-bucket", "my-org", point)

        DataFrame:
            If the ``data_frame_timestamp_column`` is not specified the index of `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_
            is used as a ``timestamp`` for written data. The index can be `PeriodIndex <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.PeriodIndex.html#pandas.PeriodIndex>`_
            or its must be transformable to ``datetime`` by
            `pandas.to_datetime <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html#pandas.to_datetime>`_.

            If you would like to transform a column to ``PeriodIndex``, you can use something like:

            .. code-block:: python

                import pandas as pd

                # DataFrame
                data_frame = ...
                # Set column as Index
                data_frame.set_index('column_name', inplace=True)
                # Transform index to PeriodIndex
                data_frame.index = pd.to_datetime(data_frame.index, unit='s')

        """  # noqa: E501

        org = org if org is not None else self.org
        bucket = bucket if bucket is not None else self.bucket

        self._append_default_tags(record)

        if write_precision is None:
            write_precision = self._write_options.write_precision

        self._write_options.validate()
        kwargs = dict(kwargs)
        no_sync, accept_partial, use_v2_api = self._resolve_write_request_options(kwargs)

        if 'tag_order' in kwargs:
            kwargs['tag_order'] = sanitize_tag_order(kwargs.get('tag_order'))
        else:
            kwargs['tag_order'] = self._write_options.tag_order

        if self._write_options.write_type is WriteType.batching:
            kwargs['no_sync'] = no_sync
            kwargs['accept_partial'] = accept_partial
            kwargs['use_v2_api'] = use_v2_api
            return self._write_batching(bucket, org, record,
                                        write_precision, **kwargs)

        payloads = defaultdict(list)
        self._serialize(record, write_precision, payloads, **kwargs)

        _async_req = True if self._write_options.write_type == WriteType.asynchronous else False

        def write_payload(payload):
            final_string = b'\n'.join(payload[1])
            return self._post_write(_async_req, bucket, org, final_string, payload[0], no_sync,
                                    accept_partial, use_v2_api, **kwargs)

        results = list(map(write_payload, payloads.items()))
        if not _async_req:
            return None
        elif len(results) == 1:
            return results[0]
        return results

    def _create_batching_pipeline(self) -> tuple[Subject[Any], rx.abc.DisposableBase]:
        """Create the batching pipeline for collecting and writing data."""
        # Define Subject that listen incoming data and produces writes into InfluxDB
        subject = Subject()

        disposable = subject.pipe(
            # Split incoming data to windows by batch_size or flush_interval
            ops.window_with_time_or_count(count=self._write_options.batch_size,
                                          timespan=datetime.timedelta(milliseconds=self._write_options.flush_interval)),
            # Map  window into groups defined by 'organization', 'bucket' and 'precision'
            ops.flat_map(lambda window: window.pipe(    # type: ignore
                # Group window by 'organization', 'bucket' and 'precision'
                ops.group_by(lambda batch_item: batch_item.key),    # type: ignore
                # Create batch (concatenation line protocols by \n)
                ops.map(lambda group: group.pipe(   # type: ignore
                    ops.to_iterable(),
                    ops.map(lambda xs: _BatchItem(key=group.key, data=_body_reduce(xs), size=len(xs))))),
                # type: ignore
                ops.merge_all())),
            # Write data into InfluxDB (possibility to retry if its fail)
            ops.filter(lambda batch: batch.size > 0),
            ops.map(mapper=lambda batch: self._to_response(data=batch, delay=self._jitter_delay())),
            ops.merge_all()) \
            .subscribe(self._on_next, self._on_error, self._on_complete)

        return subject, disposable

    def _write_batching(self, bucket, org, data,
                        precision=None,
                        **kwargs):
        if precision is None:
            precision = self._write_options.write_precision

        if isinstance(data, bytes):
            _key = _BatchItemKey(bucket, org, precision, **kwargs)
            self._subject.on_next(_BatchItem(key=_key, data=data))

        elif isinstance(data, str):
            self._write_batching(bucket, org, data.encode(_UTF_8_encoding),
                                 precision, **kwargs)

        elif isinstance(data, Point):
            self._write_batching(bucket, org,
                                 data.to_line_protocol(tag_order=kwargs.get('tag_order')),
                                 data.write_precision, **kwargs)

        elif isinstance(data, dict):
            self._write_batching(bucket, org, Point.from_dict(data, write_precision=precision, **kwargs),
                                 precision, **kwargs)

        elif 'polars' in str(type(data)):
            from influxdb_client_3.write_client.client.write.polars_dataframe_serializer \
                import PolarsDataframeSerializer
            serializer = PolarsDataframeSerializer(data,
                                                   self._point_settings, precision,
                                                   self._write_options.batch_size, **kwargs)
            for chunk_idx in range(serializer.number_of_chunks):
                self._write_batching(bucket, org,
                                     serializer.serialize(chunk_idx),
                                     precision, **kwargs)

        elif 'pandas' in str(type(data)):
            serializer = DataframeSerializer(data, self._point_settings, precision, self._write_options.batch_size,
                                             **kwargs)
            for chunk_idx in range(serializer.number_of_chunks):
                self._write_batching(bucket, org,
                                     serializer.serialize(chunk_idx),
                                     precision, **kwargs)

        elif hasattr(data, "_asdict"):
            # noinspection PyProtectedMember
            self._write_batching(bucket, org, data._asdict(), precision, **kwargs)

        elif _HAS_DATACLASS and dataclasses.is_dataclass(data):
            self._write_batching(bucket, org, dataclasses.asdict(data), precision, **kwargs)

        elif isinstance(data, Iterable):
            for item in data:
                self._write_batching(bucket, org, item, precision, **kwargs)

        elif isinstance(data, Observable):
            data.subscribe(lambda it: self._write_batching(bucket, org, it, precision, **kwargs))
            pass

        return None

    def _http(self, batch_item: _BatchItem, **kwargs):
        logger.debug("Write time series data into InfluxDB: %s", batch_item)

        if self._retry_callback:
            def _retry_callback_delegate(exception):
                return self._retry_callback(batch_item.to_key_tuple(), batch_item.data, exception)
        else:
            _retry_callback_delegate = None

        kwargs = dict(kwargs)
        no_sync, accept_partial, use_v2_api = self._resolve_write_request_options(kwargs)

        retry = self._write_options.to_retry_strategy(retry_callback=_retry_callback_delegate)

        self._post_write(False, batch_item.key.bucket, batch_item.key.org, batch_item.data,
                         batch_item.key.precision, no_sync, accept_partial, use_v2_api,
                         urlopen_kw={'retries': retry}, **kwargs)

        logger.debug("Write request finished %s", batch_item)

        return _BatchResponse(data=batch_item)

    def _post_write(self, _async_req, bucket, org, body, precision, no_sync, accept_partial, use_v2_api, **kwargs):
        # Filter out serializer-specific kwargs before passing to _post_write
        http_kwargs = {k: v for k, v in kwargs.items() if k not in SERIALIZER_KWARGS}

        # TODO refactor this
        http_kwargs['precision'] = precision
        http_kwargs['no_sync'] = no_sync
        http_kwargs['accept_partial'] = accept_partial
        http_kwargs['use_v2_api'] = use_v2_api

        #TODO Do we need this
        kwargs['_return_http_data_only'] = True

        local_var_params, path, path_params, query_params, header_params, body_params = \
            self._post_write_prepare(org, bucket, body, self.default_header, **http_kwargs)  # noqa: E501

        use_v2_api = local_var_params['use_v2_api']
        try:
            result = self.call_api(
                path, 'POST',
                query_params,
                header_params,
                body=body_params,
                async_req=local_var_params.get('async_req'),
                _request_timeout=local_var_params.get('_request_timeout'),
                urlopen_kw=http_kwargs.get('urlopen_kw', None))
            if local_var_params.get('async_req'):
                original_get = result.get

                def translated_get(timeout=None):
                    try:
                        return original_get(timeout=timeout)
                    except ApiException as e:
                        raise self._translate_write_exception(e, use_v2_api)

                result.get = translated_get
            return result
        except ApiException as e:
            raise self._translate_write_exception(e, use_v2_api)

    def __call_api(
            self, resource_path, method,
            query_params=None, header_params=None, body=None,
            _request_timeout=None, urlopen_kw=None):

        # body
        should_gzip = False
        if body:
            should_gzip = WriteApi.should_gzip(body, self.enable_gzip, self.gzip_threshold)
            body = self.sanitize_for_serialization(body)
            body = self.update_request_body(resource_path, body, should_gzip)

        # header parameters
        header_params = header_params or {}
        self.update_request_header_params(resource_path, header_params, should_gzip)
        if header_params:
            header_params = self.sanitize_for_serialization(header_params)

        # query parameters
        if query_params:
            query_params = self.sanitize_for_serialization(query_params)

        urlopen_kw = urlopen_kw or {}

        timeout = None
        _configured_timeout = _request_timeout or self.timeout
        if _configured_timeout:
            if isinstance(_configured_timeout, (int, float,)):  # noqa: E501,F821
                timeout = urllib3.Timeout(total=_configured_timeout / 1_000)
            elif (isinstance(_configured_timeout, tuple) and
                  len(_configured_timeout) == 2):
                timeout = urllib3.Timeout(
                    connect=_configured_timeout[0] / 1_000, read=_configured_timeout[1] / 1_000)

        # perform request and return response
        response_data = self.rest_client.request(
            method=method,
            url=resource_path,
            query_params=query_params,
            headers=header_params,
            body=body,
            timeout=timeout,
            **urlopen_kw
        )

        self.last_response = response_data

        return response_data

    def call_api(self, resource_path, method,
                 query_params=None, header_params=None,
                 body=None, async_req=None, _request_timeout=None, urlopen_kw=None):
        """Make the HTTP request (synchronous) and Return deserialized data.

        To make an async_req request, set the async_req parameter.

        :param resource_path: Path to method endpoint.
        :param method: Method to call.
        :param path_params: Path parameters in the url.
        :param query_params: Query parameters in the url.
        :param header_params: Header parameters to be
            placed in the request header.
        :param body: Request body.
        :param post_params dict: Request post form parameters,
            for `application/x-www-form-urlencoded`, `multipart/form-data`.
        :param auth_settings list: Auth Settings names for the request.
        :param response: Response data type.
        :param files dict: key -> filename, value -> filepath,
            for `multipart/form-data`.
        :param async_req bool: execute request asynchronously
        :param _return_http_data_only: response data without head status code
                                       and headers
        :param collection_formats: dict of collection formats for path, query,
            header, and post parameters.
        :param _preload_content: if False, the urllib3.HTTPResponse object will
                                 be returned without reading/decoding response
                                 data. Default is True.
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :param urlopen_kw: Additional parameters are passed to
                           :meth:`urllib3.request.RequestMethods.request`
        :return:
            If async_req parameter is True,
            the request will be called asynchronously.
            The method will return the request thread.
            If parameter async_req is False or missing,
            then the method will return the response directly.
        """
        if not async_req:
            return self.__call_api(resource_path, method,
                                   query_params, header_params,
                                   body, _request_timeout, urlopen_kw)

        else:
            # TODO possible refactor - async handler inside package `_sync`?
            thread = self.pool.apply_async(self.__call_api, (resource_path,
                                                             method, query_params,
                                                             header_params, body, _request_timeout, urlopen_kw))
        return thread

    # TODO cant remove because of test Ales, ask him why
    async def post_write_async(self, org, bucket, body, **kwargs):  # noqa: E501,D401,D403
        """Write data.

        Writes data to a bucket.  Use this endpoint to send data in [line protocol](https://docs.influxdata.com/influxdb/latest/reference/syntax/line-protocol/) format to InfluxDB.  #### InfluxDB Cloud  - Does the following when you send a write request:    1. Validates the request and queues the write.   2. If queued, responds with _success_ (HTTP `2xx` status code); _error_ otherwise.   3. Handles the delete asynchronously and reaches eventual consistency.    To ensure that InfluxDB Cloud handles writes and deletes in the order you request them,   wait for a success response (HTTP `2xx` status code) before you send the next request.    Because writes and deletes are asynchronous, your change might not yet be readable   when you receive the response.  #### InfluxDB OSS  - Validates the request and handles the write synchronously. - If all points were written successfully, responds with HTTP `2xx` status code;   otherwise, returns the first line that failed.  #### Required permissions  - `write-buckets` or `write-bucket BUCKET_ID`.   *`BUCKET_ID`* is the ID of the destination bucket.  #### Rate limits (with InfluxDB Cloud)  `write` rate limits apply. For more information, see [limits and adjustable quotas](https://docs.influxdata.com/influxdb/cloud/account-management/limits/).  #### Related guides  - [Write data with the InfluxDB API](https://docs.influxdata.com/influxdb/latest/write-data/developer-tools/api) - [Optimize writes to InfluxDB](https://docs.influxdata.com/influxdb/latest/write-data/best-practices/optimize-writes/) - [Troubleshoot issues writing data](https://docs.influxdata.com/influxdb/latest/write-data/troubleshoot/)
        This method makes an asynchronous HTTP request.

        :param async_req bool
        :param str org: An organization name or ID.  #### InfluxDB Cloud  - Doesn't use the `org` parameter or `orgID` parameter. - Writes data to the bucket in the organization   associated with the authorization (API token).  #### InfluxDB OSS  - Requires either the `org` parameter or the `orgID` parameter. - If you pass both `orgID` and `org`, they must both be valid. - Writes data to the bucket in the specified organization. (required)
        :param str bucket: A bucket name or ID. InfluxDB writes all points in the batch to the specified bucket. (required)
        :param str body: In the request body, provide data in [line protocol format](https://docs.influxdata.com/influxdb/latest/reference/syntax/line-protocol/).  To send compressed data, do the following:    1. Use [GZIP](https://www.gzip.org/) to compress the line protocol data.   2. In your request, send the compressed data and the      `Content-Encoding: gzip` header.  #### Related guides  - [Best practices for optimizing writes](https://docs.influxdata.com/influxdb/latest/write-data/best-practices/optimize-writes/)  (required)
        :param str zap_trace_span: OpenTracing span context
        :param str content_encoding: The compression applied to the line protocol in the request payload. To send a GZIP payload, pass `Content-Encoding: gzip` header.
        :param str content_type: The format of the data in the request body. To send a line protocol payload, pass `Content-Type: text/plain; charset=utf-8`.
        :param int content_length: The size of the entity-body, in bytes, sent to InfluxDB. If the length is greater than the `max body` configuration option, the server responds with status code `413`.
        :param str accept: The content type that the client can understand. Writes only return a response body if they fail--for example, due to a formatting problem or quota limit.  #### InfluxDB Cloud    - Returns only `application/json` for format and limit errors.   - Returns only `text/html` for some quota limit errors.  #### InfluxDB OSS    - Returns only `application/json` for format and limit errors.  #### Related guides  - [Troubleshoot issues writing data](https://docs.influxdata.com/influxdb/latest/write-data/troubleshoot/)
        :param str org_id: An organization ID.  #### InfluxDB Cloud  - Doesn't use the `org` parameter or `orgID` parameter. - Writes data to the bucket in the organization   associated with the authorization (API token).  #### InfluxDB OSS  - Requires either the `org` parameter or the `orgID` parameter. - If you pass both `orgID` and `org`, they must both be valid. - Writes data to the bucket in the specified organization.
        :param WritePrecision precision: The precision for unix timestamps in the line protocol batch.
        :param bool no_sync: Instructs the server whether to wait with the response until WAL persistence completes. True value means faster write but without the confirmation that the data was persisted. Note: This option is supported by InfluxDB 3 Core and Enterprise servers only. For other InfluxDB 3 server types (InfluxDB Clustered, InfluxDB Clould Serverless/Dedicated) the write operation will fail with an error.
        :return: None
                 If the method is called asynchronously,
                 returns the request thread.
        """  # noqa: E501
        local_var_params, path, path_params, query_params, header_params, body_params = \
            self._post_write_prepare(org, bucket, body, **kwargs)  # noqa: E501
        use_v2_api = local_var_params['use_v2_api']

        try:
            return await self.call_api(
                resource_path=path,
                method='POST',
                query_params=query_params,
                header_params=header_params,
                body=body,
                async_req=local_var_params.get('async_req'),
                _request_timeout=local_var_params.get('_request_timeout'),
                urlopen_kw=kwargs.get('urlopen_kw', None))
        except ApiException as e:
            raise self._translate_write_exception(e, use_v2_api)

    # ------------------------------------------------------------

    def _post_write_prepare(self, org, bucket, body, default_header, **kwargs):  # noqa: E501,D401,D403
        local_var_params = dict(locals())

        all_params = ['org', 'bucket', 'body', 'zap_trace_span', 'content_encoding', 'content_type', 'content_length',
                      'accept', 'org_id', 'precision', 'no_sync', 'accept_partial', 'use_v2_api']  # noqa: E501
        self._check_operation_params('post_write', all_params, local_var_params)
        local_var_params.setdefault('use_v2_api', DEFAULT_WRITE_USE_V2_API)
        local_var_params.setdefault('no_sync', DEFAULT_WRITE_NO_SYNC)
        local_var_params.setdefault('accept_partial', DEFAULT_WRITE_ACCEPT_PARTIAL)
        # verify the required parameter 'org' is set
        if ('org' not in local_var_params or
                local_var_params['org'] is None):
            raise ValueError("Missing the required parameter `org` when calling `post_write`")  # noqa: E501
        # verify the required parameter 'bucket' is set
        if ('bucket' not in local_var_params or
                local_var_params['bucket'] is None):
            raise ValueError("Missing the required parameter `bucket` when calling `post_write`")  # noqa: E501
        # verify the required parameter 'body' is set
        if ('body' not in local_var_params or
                local_var_params['body'] is None):
            raise ValueError("Missing the required parameter `body` when calling `post_write`")  # noqa: E501

        path_params = {}
        query_params = []

        use_v2_api = local_var_params['use_v2_api']
        no_sync = local_var_params['no_sync']
        accept_partial = local_var_params['accept_partial']
        if 'org' in local_var_params:
            query_params.append(('org', local_var_params['org']))  # noqa: E501
        if 'org_id' in local_var_params:
            query_params.append(('orgID', local_var_params['org_id']))  # noqa: E501
        if 'bucket' in local_var_params:
            query_params.append(('bucket' if use_v2_api else 'db', local_var_params['bucket']))  # noqa: E501

        if use_v2_api:
            path = '/api/v2/write'
            if 'precision' in local_var_params:
                precision = local_var_params['precision']
                query_params.append(('precision', WritePrecisionConverter.to_v2_api_string(precision)))  # noqa: E501
        else:
            path = '/api/v3/write_lp'
            if 'precision' in local_var_params:
                precision = local_var_params['precision']
                query_params.append(('precision', WritePrecisionConverter.to_v3_api_string(precision)))  # noqa: E501
            if no_sync:
                query_params.append(('no_sync', 'true'))
            if accept_partial is False:
                query_params.append(('accept_partial', 'false'))

        header_params = default_header if default_header is not None else {}

        if 'content_encoding' in local_var_params:
            header_params['Content-Encoding'] = local_var_params['content_encoding']  # noqa: E501

        if 'content_type' in local_var_params:
            header_params['Content-Type'] = 'text/plain; charset=utf-8'

        body_params = None
        if 'body' in local_var_params:
            body_params = local_var_params['body']

        return local_var_params, path, path_params, query_params, header_params, body_params

    @property
    def pool(self):
        """Create thread pool on first request avoids instantiating unused threadpool for blocking clients."""
        if self._pool is None:
            self._pool = ThreadPool(self.pool_threads)
        return self._pool

    def _check_operation_params(self, operation_id, supported_params, local_params):
        supported_params.append('async_req')
        supported_params.append('_return_http_data_only')
        supported_params.append('_preload_content')
        supported_params.append('_request_timeout')
        supported_params.append('urlopen_kw')
        for key, val in local_params['kwargs'].items():
            if key not in supported_params:
                raise TypeError(
                    f"Got an unexpected keyword argument '{key}'"
                    f" to method {operation_id}"
                )
            local_params[key] = val
        del local_params['kwargs']

    def update_request_header_params(self, path: str, params: dict, should_gzip: bool = False):
        if should_gzip:
            # GZIP Request
            if path == '/api/v2/write' or path == '/api/v3/write_lp':
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

    def update_request_body(self, path: str, body, should_gzip: bool = False):
        # TODO why
        _body = body
        if should_gzip:
            # GZIP Request
            if path == '/api/v2/write' or path == '/api/v3/write_lp':
                import gzip
                if isinstance(_body, bytes):
                    return gzip.compress(data=_body)
                else:
                    return gzip.compress(bytes(_body, _UTF_8_encoding))

        return _body

    def flush(self):
        """
        Flush any buffered writes to InfluxDB without closing the client.

        This method immediately sends all buffered data points to the server
        when using batching write mode. After flushing, the client remains
        open and ready for more writes.

        For synchronous or asynchronous write modes, this is a no-op since
        data is written immediately.
        """
        if self._write_options.write_type is not WriteType.batching:
            return  # Nothing to flush for synchronous/asynchronous writes

        self.close()  # Close existing batching pipeline

        # Recreate the batching pipeline for continued use
        self._subject, self._disposable = self._create_batching_pipeline()

    def close(self):
        """Flush data and dispose a batching buffer."""
        if self._subject is None:
            return  # Already closed

        self._subject.on_completed()
        self._subject.dispose()
        self._subject = None

        """
        We impose a maximum wait time to ensure that we do not cause a deadlock if the
        background thread has exited abnormally

        Each iteration waits 100ms, but sleep expects the unit to be seconds so convert
        the maximum wait time to seconds.

        We keep a counter of how long we've waited
        """
        max_wait_time = self._write_options.max_close_wait / 1000
        waited = 0
        sleep_period = 0.1

        # Wait for writing to finish
        while not self._disposable.is_disposed:
            sleep(sleep_period)
            waited += sleep_period

            # Have we reached the upper limit?
            if waited >= max_wait_time:
                logger.warning(
                    "Reached max_close_wait (%s seconds) waiting for batches to finish writing. Force closing",
                    max_wait_time
                )
                break

        if self._disposable:
            self._disposable = None

    def sanitize_for_serialization(self, obj):
        """Build a JSON POST object.

        If obj is None, return None.
        If obj is str, int, long, float, bool, return directly.
        If obj is datetime.datetime, datetime.date
            convert to string in iso8601 format.
        If obj is list, sanitize each element in the list.
        If obj is dict, return the dict.
        If obj is OpenAPI model, return the properties dict.

        :param obj: The data to serialize.
        :return: The serialized form of data.
        """
        if obj is None:
            return None
        elif isinstance(obj, self.PRIMITIVE_TYPES):
            return obj
        elif isinstance(obj, list):
            return [self.sanitize_for_serialization(sub_obj)
                    for sub_obj in obj]
        elif isinstance(obj, tuple):
            return tuple(self.sanitize_for_serialization(sub_obj)
                         for sub_obj in obj)
        elif isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()

        if isinstance(obj, dict):
            obj_dict = obj
        else:
            # Convert model obj to dict except
            # attributes `openapi_types`, `attribute_map`
            # and attributes which value is not None.
            # Convert attribute name to json key in
            # model definition for request.
            obj_dict = {obj.attribute_map[attr]: getattr(obj, attr)
                        for attr, _ in obj.openapi_types.items()
                        if getattr(obj, attr) is not None}

        return {key: self.sanitize_for_serialization(val)
                for key, val in obj_dict.items()}

    @staticmethod
    def _translate_write_exception(exc, use_v2_api):
        if use_v2_api and exc.status == HTTPStatus.METHOD_NOT_ALLOWED:
            message = ("Server doesn't support the V2 API endpoint (/api/v2/write). "
                       "Set use_v2_api=False to use the V3 API endpoint.")
            ex = ApiException(status=0, reason=message)
            ex.message = message
            ex.args = (message,)
            return ex
        if not use_v2_api and exc.status == HTTPStatus.METHOD_NOT_ALLOWED:
            message = ("Server doesn't support the V3 API endpoint (/api/v3/write_lp). "
                       "Set use_v2_api=True to use the V2 API endpoint.")
            ex = ApiException(status=0, reason=message)
            ex.message = message
            ex.args = (message,)
            return ex
        partial = InfluxDBPartialWriteError.from_response(exc.response)
        if partial is not None:
            return partial
        return exc

    @staticmethod
    def should_gzip(payload: str, enable_gzip: bool = False, gzip_threshold: int = None) -> bool:
        """
        Determines whether gzip compression should be applied to the given payload based
        on the specified conditions. This method evaluates the `enable_gzip` flag and
        considers the size of the payload in relation to the optional `gzip_threshold`.
        If `enable_gzip` is set to True and no threshold is provided, gzip compression
        is advised without any size condition. If a threshold is specified, compression
        is applied only when the size of the payload meets or exceeds the threshold.
        By default, no compression is performed if `enable_gzip` is False.

        :param payload: The payload data as a string for which gzip determination is to
            be made.
        :type payload: str
        :param enable_gzip: A flag indicating whether gzip compression is enabled. By
            default, this flag is False.
        :type enable_gzip: bool, optional
        :param gzip_threshold: Optional threshold specifying the minimum size (in bytes)
            of the payload to trigger gzip compression. Only considered if
            `enable_gzip` is True.
        :type gzip_threshold: int, optional
        :return: A boolean value indicating True if gzip compression should be applied
            based on the payload size, the enable_gzip flag, and the gzip_threshold.
        :rtype: bool
        """
        if enable_gzip is not False:
            if gzip_threshold is not None:
                payload_size = len(payload.encode('utf-8'))
                return payload_size >= gzip_threshold
            if enable_gzip is True:
                return True

        return False

    @staticmethod
    def _on_error(ex):
        logger.error("unexpected error during batching: %s", ex)

    def _to_response(self, data: _BatchItem, delay: datetime.timedelta):
        return rx.of(data).pipe(
            ops.subscribe_on(self._write_options.write_scheduler),
            # use delay if its specified
            ops.delay(duetime=delay, scheduler=self._write_options.write_scheduler),
            # invoke http call
            ops.map(lambda x: self._http(x, **x.key.kwargs)),
            # catch exception to fail batch response
            ops.catch(handler=lambda exception, source: rx.just(_BatchResponse(exception=exception, data=data))),
        )

    def _on_next(self, response: _BatchResponse):
        if response.exception:
            logger.error("The batch item wasn't processed successfully because: %s", response.exception)
            if self._error_callback:
                try:
                    self._error_callback(response.data.to_key_tuple(), response.data.data, response.exception)
                except Exception as e:
                    """
                    Unfortunately, because callbacks are user-provided generic code, exceptions can be entirely
                    arbitrary

                    We trap it, log that it occurred and then proceed - there's not much more that we can
                    really do.
                    """
                    logger.error("The configured error callback threw an exception: %s", e)

        else:
            logger.debug("The batch item: %s was processed successfully.", response)
            if self._success_callback:
                try:
                    self._success_callback(response.data.to_key_tuple(), response.data.data)
                except Exception as e:
                    logger.error("The configured success callback threw an exception: %s", e)

    def _on_complete(self):
        self._disposable.dispose()
        logger.debug("the batching processor was disposed")

    def _append_default_tag(self, key, val, record):
        from influxdb_client_3.write_client import Point
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

    def _resolve_write_request_options(self, kwargs):
        no_sync = kwargs.pop('no_sync', self._write_options.no_sync)
        accept_partial = kwargs.pop('accept_partial', self._write_options.accept_partial)
        use_v2_api = kwargs.pop('use_v2_api', self._write_options.use_v2_api)
        if use_v2_api and no_sync:
            raise ValueError("invalid write options: no_sync cannot be used with use_v2_api")
        return no_sync, accept_partial, use_v2_api

    def _jitter_delay(self):
        return datetime.timedelta(milliseconds=random() * self._write_options.jitter_interval)

    def _serialize(self, record, write_precision, payload, **kwargs):
        from influxdb_client_3.write_client.client.write.point import Point
        if isinstance(record, bytes):
            payload[write_precision].append(record)

        elif isinstance(record, str):
            self._serialize(record.encode(_UTF_8_encoding), write_precision, payload, **kwargs)

        elif isinstance(record, Point):
            precision_from_point = kwargs.get('precision_from_point', True)
            precision = record.write_precision if precision_from_point else write_precision
            self._serialize(record.to_line_protocol(precision=precision, tag_order=kwargs.get('tag_order')),
                            precision, payload, **kwargs)

        elif isinstance(record, dict):
            self._serialize(Point.from_dict(record, write_precision=write_precision, **kwargs),
                            write_precision, payload, **kwargs)
        elif 'polars' in str(type(record)):
            from influxdb_client_3.write_client.client.write.polars_dataframe_serializer import \
                PolarsDataframeSerializer
            serializer = PolarsDataframeSerializer(record, self._point_settings, write_precision, **kwargs)
            self._serialize(serializer.serialize(), write_precision, payload, **kwargs)

        elif 'pandas' in str(type(record)):
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

    def __enter__(self):
        """
        Enter the runtime context related to this object.

        It will bind this method’s return value to the target(s)
        specified in the `as` clause of the statement.

        return: self instance
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object and close the WriteApi."""
        self.close()

    def __del__(self):
        """Close WriteApi."""
        self.close()

    def __getstate__(self):
        """Return a dict of attributes that you want to pickle."""
        state = self.__dict__.copy()
        # Remove rx
        del state['_subject']
        del state['_disposable']
        del state['_write_service']
        return state

    def __setstate__(self, state):
        """Set your object with the provided dict."""
        self.__dict__.update(state)
        # Init Rx
        self.__init__(self._write_options,
                      self._point_settings,
                      success_callback=self._success_callback,
                      error_callback=self._error_callback,
                      retry_callback=self._retry_callback)