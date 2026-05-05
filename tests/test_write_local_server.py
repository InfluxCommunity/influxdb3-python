import re
import time
from http import HTTPStatus

import pandas as pd
import pytest
from pytest_httpserver import HTTPServer, RequestMatcher
from urllib3.exceptions import TimeoutError as urllib3_TimeoutError

from influxdb_client_3 import InfluxDBClient3, WriteOptions, WritePrecision, write_client_options, WriteType
from influxdb_client_3.write_client.rest import ApiException


class TestWriteLocalServer:
    SAMPLE_RECORD = "mem,tag=one value=1.0"

    @staticmethod
    def set_response_status(httpserver, response_status_code):
        httpserver.expect_request(re.compile(".*")).respond_with_data(status=response_status_code)

    @staticmethod
    def assert_request_made(httpserver, matcher):
        httpserver.assert_request_made(matcher)
        httpserver.check_assertions()

    @staticmethod
    def delay_response(httpserver: HTTPServer, delay=1.0):
        httpserver.expect_request(re.compile(".*")).respond_with_handler(lambda request: time.sleep(delay))

    def test_write_default_params(self, httpserver: HTTPServer):
        self.set_response_status(httpserver, 200)

        InfluxDBClient3(
            host=(httpserver.url_for("/")), org="ORG", database="DB", token="TOKEN",
            write_client_options=write_client_options(
                write_options=WriteOptions(write_type=WriteType.synchronous)
            )
        ).write(self.SAMPLE_RECORD)

        self.assert_request_made(httpserver, RequestMatcher(
            method="POST", uri="/api/v3/write_lp",
            query_string={"org": "ORG", "db": "DB", "precision": "nanosecond"}))

    def test_write_with_write_options(self, httpserver: HTTPServer):
        self.set_response_status(httpserver, 200)

        InfluxDBClient3(
            host=(httpserver.url_for("/")), org="ORG", database="DB", token="TOKEN",
            write_client_options=write_client_options(
                write_options=WriteOptions(
                    write_type=WriteType.synchronous,
                    write_precision=WritePrecision.US,
                    no_sync=False
                )
            ),
        ).write(self.SAMPLE_RECORD)

        self.assert_request_made(httpserver, RequestMatcher(
            method="POST", uri="/api/v3/write_lp",
            query_string={"org": "ORG", "db": "DB", "precision": "microsecond"}))

    def test_write_with_no_sync_true(self, httpserver: HTTPServer):
        self.set_response_status(httpserver, 200)

        InfluxDBClient3(
            host=(httpserver.url_for("/")), org="ORG", database="DB", token="TOKEN",
            write_client_options=write_client_options(
                write_options=WriteOptions(
                    write_type=WriteType.synchronous,
                    write_precision=WritePrecision.US,
                    no_sync=True
                )
            )
        ).write(self.SAMPLE_RECORD)

        self.assert_request_made(httpserver, RequestMatcher(
            method="POST", uri="/api/v3/write_lp",
            query_string={"org": "ORG", "db": "DB", "precision": "microsecond", "no_sync": "true"}))

    def test_write_with_accept_partial_false(self, httpserver: HTTPServer):
        self.set_response_status(httpserver, 200)

        InfluxDBClient3(
            host=(httpserver.url_for("/")), org="ORG", database="DB", token="TOKEN",
            write_client_options=write_client_options(
                write_options=WriteOptions(
                    write_type=WriteType.synchronous,
                    accept_partial=False
                )
            )
        ).write(self.SAMPLE_RECORD)

        self.assert_request_made(httpserver, RequestMatcher(
            method="POST", uri="/api/v3/write_lp",
            query_string={"org": "ORG", "db": "DB", "precision": "nanosecond", "accept_partial": "false"}))

    def test_write_with_use_v2_api_true(self, httpserver: HTTPServer):
        self.set_response_status(httpserver, 200)

        InfluxDBClient3(
            host=(httpserver.url_for("/")), org="ORG", database="DB", token="TOKEN",
            write_client_options=write_client_options(
                write_options=WriteOptions(
                    write_type=WriteType.synchronous,
                    write_precision=WritePrecision.US,
                    use_v2_api=True,
                    accept_partial=False
                )
            )
        ).write(self.SAMPLE_RECORD)

        self.assert_request_made(httpserver, RequestMatcher(
            method="POST", uri="/api/v2/write",
            query_string={"org": "ORG", "bucket": "DB", "precision": "us"}))

    def test_write_with_v3_on_v2_server(self, httpserver: HTTPServer):
        self.set_response_status(httpserver, HTTPStatus.METHOD_NOT_ALLOWED)

        client = InfluxDBClient3(
            host=(httpserver.url_for("/")), org="ORG", database="DB", token="TOKEN")

        with pytest.raises(ApiException, match=r".*Server doesn't support v3 write API\. "
                                               r"Set use_v2_api=True for v2 compatibility endpoint\."):
            client.write(self.SAMPLE_RECORD)

        self.assert_request_made(httpserver, RequestMatcher(
            method="POST", uri="/api/v3/write_lp",
            query_string={"org": "ORG", "db": "DB", "precision": "nanosecond"}))

    def test_write_with_no_sync_false_and_gzip(self, httpserver: HTTPServer):
        self.set_response_status(httpserver, 200)

        InfluxDBClient3(
            host=(httpserver.url_for("/")), org="ORG", database="DB", token="TOKEN",
            write_client_options=write_client_options(
                write_options=WriteOptions(
                    write_type=WriteType.synchronous,
                    write_precision=WritePrecision.US,
                    no_sync=False
                )
            ),
            enable_gzip=True
        ).write(self.SAMPLE_RECORD)

        self.assert_request_made(httpserver, RequestMatcher(
            method="POST", uri="/api/v3/write_lp",
            query_string={"org": "ORG", "db": "DB", "precision": "microsecond"},
            headers={"Content-Encoding": "gzip"}, ))

    def test_write_with_no_sync_true_and_gzip(self, httpserver: HTTPServer):
        self.set_response_status(httpserver, 200)

        InfluxDBClient3(
            host=(httpserver.url_for("/")), org="ORG", database="DB", token="TOKEN",
            write_client_options=write_client_options(
                write_options=WriteOptions(
                    write_type=WriteType.synchronous,
                    write_precision=WritePrecision.US,
                    no_sync=True
                )
            ),
            enable_gzip=True
        ).write(self.SAMPLE_RECORD)

        self.assert_request_made(httpserver, RequestMatcher(
            method="POST", uri="/api/v3/write_lp",
            query_string={"org": "ORG", "db": "DB", "precision": "microsecond", "no_sync": "true"},
            headers={"Content-Encoding": "gzip"}, ))

    def test_write_invalid_use_v2_api_and_no_sync(self, httpserver: HTTPServer):
        client = InfluxDBClient3(
            host=(httpserver.url_for("/")), org="ORG", database="DB", token="TOKEN",
            write_client_options=write_client_options(
                write_options=WriteOptions(
                    write_type=WriteType.synchronous,
                    use_v2_api=True,
                    no_sync=True
                )
            )
        )
        with pytest.raises(ValueError, match=r".*invalid write options: no_sync cannot be used with use_v2_api.*"):
            client.write(self.SAMPLE_RECORD)

    def test_write_with_timeout_in_write_options(self, httpserver: HTTPServer):
        self.delay_response(httpserver, 0.5)

        with pytest.raises(urllib3_TimeoutError):
            InfluxDBClient3(
                host=(httpserver.url_for("/")), org="ORG", database="DB", token="TOKEN",
                write_client_options=write_client_options(
                    write_options=WriteOptions(
                        write_type=WriteType.synchronous,
                        write_precision=WritePrecision.US,
                        timeout=30,
                        no_sync=True
                    )
                ),
                enable_gzip=True
            ).write(self.SAMPLE_RECORD)

    def test_write_with_write_timeout(self, httpserver: HTTPServer):
        self.delay_response(httpserver, 0.5)

        with pytest.raises(urllib3_TimeoutError):
            InfluxDBClient3(
                host=(httpserver.url_for("/")), org="ORG", database="DB", token="TOKEN",
                write_timeout=30,
                write_client_options=write_client_options(
                    write_options=WriteOptions(
                        write_type=WriteType.synchronous,
                        write_precision=WritePrecision.US,
                        no_sync=True,
                    )
                ),
                enable_gzip=True
            ).write(self.SAMPLE_RECORD)

    def test_write_with_timeout_arg(self, httpserver: HTTPServer):
        self.delay_response(httpserver, 0.5)

        with pytest.raises(urllib3_TimeoutError):
            InfluxDBClient3(
                host=(httpserver.url_for("/")), org="ORG", database="DB", token="TOKEN",
                write_client_options=write_client_options(
                    write_options=WriteOptions(
                        write_type=WriteType.synchronous,
                        write_precision=WritePrecision.US,
                        no_sync=True,
                    )
                ),
                enable_gzip=True
            ).write(self.SAMPLE_RECORD, _request_timeout=1)

    def test_write_dataframe_does_not_raise_type_error(self, httpserver: HTTPServer):
        """
        Regression test: writing a DataFrame should not raise TypeError.

        Before the fix, serializer kwargs were passed to post_write(), causing a TypeError.
        """
        self.set_response_status(httpserver, 200)

        df = pd.DataFrame({
            'time': pd.to_datetime(['2024-01-01', '2024-01-02']),
            'city': ['London', 'Paris'],
            'temperature': [15.0, 18.0]
        })

        try:
            InfluxDBClient3(
                host=(httpserver.url_for("/")), org="ORG", database="DB", token="TOKEN",
                write_client_options=write_client_options(
                    write_options=WriteOptions(write_type=WriteType.synchronous)
                )
            ).write_dataframe(
                df,
                measurement='weather',
                timestamp_column='time',
                tags=['city']
            )
        except TypeError as e:
            pytest.fail(f"write_dataframe raised TypeError: {e}")
