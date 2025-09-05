import re
import time
from http import HTTPStatus

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
    def delay_response(httpserver, delay=1.0):
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
            method="POST", uri="/api/v2/write",
            query_string={"org": "ORG", "bucket": "DB", "precision": "ns"}))

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
            method="POST", uri="/api/v2/write",
            query_string={"org": "ORG", "bucket": "DB", "precision": "us"}))

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

    def test_write_with_no_sync_true_on_v2_server(self, httpserver: HTTPServer):
        self.set_response_status(httpserver, HTTPStatus.METHOD_NOT_ALLOWED)

        client = InfluxDBClient3(
            host=(httpserver.url_for("/")), org="ORG", database="DB", token="TOKEN",
            write_client_options=write_client_options(
                write_options=WriteOptions(
                    write_type=WriteType.synchronous,
                    no_sync=True)))

        with pytest.raises(ApiException, match=r".*Server doesn't support write with no_sync=true "
                                               r"\(supported by InfluxDB 3 Core/Enterprise servers only\)."):
            client.write(self.SAMPLE_RECORD)

        self.assert_request_made(httpserver, RequestMatcher(
            method="POST", uri="/api/v3/write_lp",
            query_string={"org": "ORG", "db": "DB", "precision": "nanosecond", "no_sync": "true"}))

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
            method="POST", uri="/api/v2/write",
            query_string={"org": "ORG", "bucket": "DB", "precision": "us"},
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
        self.set_response_status(httpserver, 200)

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
