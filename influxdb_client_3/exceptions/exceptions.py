"""Exceptions utils for InfluxDB."""

import json
import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple

from urllib3 import HTTPResponse

logger = logging.getLogger('influxdb_client_3.exceptions')


class InfluxDB3ClientError(Exception):
    """
    Exception raised for errors in the InfluxDB client operations.

    Represents errors that occur during interactions with the InfluxDB
    database client. This exception is a general base class for more
    specific client-related failures and is typically used to signal issues
    such as invalid queries, connection failures, or API misusage.
    """
    pass


# This error is for all query operations
class InfluxDB3ClientQueryError(InfluxDB3ClientError):
    """
    Represents an error that occurs when querying an InfluxDB client.

    This class is specifically designed to handle errors originating from
    client queries to an InfluxDB database. It extends the general
    `InfluxDBClientError`, allowing more precise identification and
    handling of query-related issues.

    :ivar message: Contains the specific error message describing the
        query error.
    :type message: str
    """

    def __init__(self, error_message, *args, **kwargs):
        super().__init__(error_message, *args, **kwargs)
        self.message = error_message


def _is_partial_write_error(error_message) -> bool:
    if not isinstance(error_message, str) or len(error_message) == 0:
        return False
    normalized = error_message.lower()
    return (
        "partial write of line protocol occurred" in normalized or
        "parsing failed for write_lp endpoint" in normalized
    )


def _parse_partial_write_data_item(item) -> Optional[Tuple[str, int, str]]:
    if item is None:
        return None
    if not isinstance(item, dict):
        raise ValueError("array item is not an object")

    error_message = item.get("error_message")
    if not isinstance(error_message, str):
        raise ValueError("error_message must be string")
    if len(error_message) == 0:
        return None

    line_number_raw = item.get("line_number")
    if line_number_raw is None:
        line_number = 0
    elif isinstance(line_number_raw, int):
        line_number = line_number_raw
    else:
        raise ValueError("line_number must be int")

    original_line_raw = item.get("original_line")
    if original_line_raw is None:
        original_line = ""
    elif isinstance(original_line_raw, str):
        original_line = original_line_raw
    else:
        raise ValueError("original_line must be string")

    return error_message, line_number, original_line


def _parse_typed_partial_write_array(data) -> Optional[List[Tuple[str, int, str]]]:
    if not isinstance(data, list):
        return None
    line_errors: List[Tuple[str, int, str]] = []
    try:
        for item in data:
            parsed = _parse_partial_write_data_item(item)
            if parsed is None:
                continue
            line_errors.append(parsed)
    except ValueError:
        return None
    return line_errors if len(line_errors) > 0 else None


def _parse_raw_array_details(data) -> Optional[List[str]]:
    if not isinstance(data, list):
        return None
    details: List[str] = []
    for item in data:
        if item is None:
            continue
        raw = json.dumps(item, separators=(',', ':'))
        if raw and raw.lower() != "null":
            details.append(raw)
    return details


def _parse_typed_partial_write_object(data) -> Optional[Tuple[str, int, str]]:
    if data is None:
        return None
    try:
        return _parse_partial_write_data_item(data)
    except ValueError:
        return None


def _format_partial_write_details(line_errors: List[Tuple[str, int, str]]) -> List[str]:
    details: List[str] = []
    for error_message, line_number, original_line in line_errors:
        if line_number != 0 and original_line != "":
            details.append(f"\tline {line_number}: {error_message} ({original_line})")
        elif error_message:
            details.append(f"\t{error_message}")
    return details


def _parse_partial_write_line_error_info(data) -> Tuple[List[Tuple[str, int, str]], List[str]]:
    if data is None:
        return [], []

    typed_array = _parse_typed_partial_write_array(data)
    if typed_array is not None:
        return typed_array, _format_partial_write_details(typed_array)

    raw_details = _parse_raw_array_details(data)
    if raw_details is not None:
        return [], raw_details

    typed_single = _parse_typed_partial_write_object(data)
    if typed_single is not None:
        line_errors = [typed_single]
        return line_errors, _format_partial_write_details(line_errors)

    return [], []


# This error is for all write operations
class InfluxDBError(InfluxDB3ClientError):
    """Raised when a server error occurs."""

    def __init__(self, response: HTTPResponse = None, message: str = None):
        """Initialize the InfluxDBError handler."""
        if response is not None:
            self.response = response
            self.message = self._get_message(response)
            self.retry_after = response.getheader('Retry-After')
        else:
            self.response = None
            self.message = message or 'no response'
            self.retry_after = None
        super().__init__(self.message)

    def _get_message(self, response):
        if response.data:
            def get(d, key):
                if not key or d is None:
                    return d
                if not isinstance(d, dict):
                    return None
                return get(d.get(key[0]), key[1:])
            try:
                node = json.loads(response.data)
                if isinstance(node, dict):
                    # InfluxDB v3 error format: { "code": "...", "message": "..." }
                    code = node.get("code")
                    message = node.get("message")
                    if message:
                        return f"{code}: {message}" if code else message
                    # InfluxDB v3 write error format:
                    # {
                    #   "error": "...",
                    #   "data": [ { "error_message": "...", "line_number": 2, "original_line": "..." }, ... ]
                    # }
                    error_text = node.get("error")
                    if error_text and _is_partial_write_error(error_text):
                        _, details = _parse_partial_write_line_error_info(node.get("data"))
                        if details:
                            return error_text + ":\n" + "\n".join(
                                detail if detail.startswith("\t") else f"\t{detail}"
                                for detail in details
                            )
                        return error_text
                    if error_text:
                        return error_text
                for key in [['message'], ['data', 'error_message'], ['error']]:
                    value = get(node, key)
                    if value is not None:
                        return value
                return response.data
            except Exception as e:
                logging.debug(f"Cannot parse error response to JSON: {response.data}, {e}")
                return response.data

        # Header
        for header_key in ["X-Platform-Error-Code", "X-Influx-Error", "X-InfluxDb-Error"]:
            header_value = response.getheader(header_key)
            if header_value is not None:
                return header_value

        # Http Status
        return response.reason

    def getheaders(self):
        """Helper method to make response headers more accessible."""
        return self.response.getheaders()


@dataclass(frozen=True)
class InfluxDBPartialWriteLineError:
    line_number: int
    error_message: str
    original_line: str


class InfluxDBPartialWriteError(InfluxDBError):
    """Structured partial-write error with per-line failures."""

    def __init__(self, response: HTTPResponse, message: str, line_errors: List[InfluxDBPartialWriteLineError]):
        super().__init__(response=response)
        self.message = message
        self.line_errors = line_errors
        self.args = (self.message,)

    @classmethod
    def from_response(cls, response: HTTPResponse):
        if response is None or not response.data:
            return None
        try:
            node = json.loads(response.data)
        except Exception:
            return None
        if not isinstance(node, dict):
            return None
        error_text = node.get("error")
        if not _is_partial_write_error(error_text):
            return None
        parsed_line_errors, _ = _parse_partial_write_line_error_info(node.get("data"))
        if len(parsed_line_errors) == 0:
            return None
        line_errors = [
            InfluxDBPartialWriteLineError(
                line_number=line_number,
                error_message=error_message,
                original_line=original_line,
            )
            for error_message, line_number, original_line in parsed_line_errors
        ]
        message = InfluxDBError(response=response).message
        return cls(response=response, message=message, line_errors=line_errors)
