# coding: utf-8

from __future__ import absolute_import

import json
import logging
from http import HTTPStatus
from json import JSONDecodeError
from typing import Union, Optional, Any, Tuple, List

from influxdb_client_3.exceptions import InfluxDBError, InfluxDBPartialWriteError, InfluxDBPartialWriteLineError

_UTF_8_encoding = 'utf-8'

logger = logging.getLogger('influxdb_client_3.write_client.write_exceptions')


class ApiException(InfluxDBError):

    def __init__(self, status=None, reason=None, http_resp=None):
        """Initialize with HTTP response."""
        super().__init__(response=http_resp)
        if http_resp:
            self.status = http_resp.status
            self.reason = http_resp.reason
            self.body = http_resp.data
            self.headers = http_resp.getheaders()
        else:
            self.status = status
            self.reason = reason
            self.body = None
            self.headers = None

    def __str__(self):
        """Get custom error messages for exception."""
        error_message = "({0})\n" \
                        "Reason: {1}\n".format(self.status, self.reason)
        if self.headers:
            error_message += "HTTP response headers: {0}\n".format(
                self.headers)

        if self.body:
            error_message += "HTTP response body: {0}\n".format(self.body)

        return error_message


def translate_write_exception(
        exc: ApiException,
        use_v2_api=False,
        accept_partial=False,
) -> Union[ApiException, InfluxDBPartialWriteError]:
    if exc.status == HTTPStatus.METHOD_NOT_ALLOWED:
        return create_unsupported_endpoint_exception(use_v2_api)

    root = parse_json(exc.body)
    if (root is None or
            root == "" or
            isinstance(root, dict) is False or
            (isinstance(root, dict) and (not root.get("error") and not root.get("message")))):
        message = extract_fallback_reason(exc.response)
        exc.message = message
        return exc

    if isinstance(root, dict) and is_partial_write_error(exc.status, use_v2_api, accept_partial, root):
        # InfluxDB 3 Core/Enterprise partial write error format:
        # {"error":"...","data":[{"error_message":"...","line_number":2,"original_line": "..."}]}
        return handle_partial_write_error(exc.response, root)

    exc.message = get_message(root, exc.response)
    return exc


def get_message(root, response):
    if root:
        try:
            if isinstance(root, dict):
                # InfluxDB v3 error format: { "code": "...", "message": "..." }
                message = root.get("message")
                if message:
                    code = root.get("code")
                    return f"{code}: {message}" if code else message

                # Core/Enterprise object format:
                # {"error":"...","data":{"error_message":"..."}}
                error_text = root.get("error")
                if error_text:
                    data = root.get("data")
                    if isinstance(data, dict):
                        line_error = parse_typed_partial_write_object_or_none(data)
                        if line_error is not None and line_error.error_message:
                            return f"{error_text}:\n\t{format_line_error(line_error)}"
                    return error_text
        except Exception as e:
            logger.debug("Cannot parse error response to JSON: %s, %s", response.data, e)
            return response.data


def parse_json(json_str: Optional[Union[str, bytes]]) -> Optional[Any]:
    if not json_str:
        return None
    try:
        return json.loads(json_str)
    except (JSONDecodeError, TypeError, ValueError) as e:
        logger.debug("Can't parse msg from response body %s: %s", json_str, e)
        return None


def create_unsupported_endpoint_exception(use_v2_api: bool) -> ApiException:
    if use_v2_api:
        message = ("Server doesn't support the V2 API endpoint (/api/v2/write). "
                   "Set use_v2_api=False to use the V3 API endpoint.")
    else:
        message = ("Server doesn't support the V3 API endpoint (/api/v3/write_lp). "
                   "Set use_v2_api=True to use the V2 API endpoint.")
    ex = ApiException(status=0, reason=message)
    ex.message = message
    ex.args = (message,)
    return ex


def is_partial_write_error(status_code, use_v2_api, accept_partial, root) -> bool:
    return (status_code == HTTPStatus.BAD_REQUEST and
            accept_partial is True and
            use_v2_api is False and
            (isinstance(root.get('data'), list) and len(root.get('data')) > 0)
            )


def handle_partial_write_error(response, root: dict) -> InfluxDBPartialWriteError:
    reason = root.get("error") or ""
    parse_res = parse_partial_write_line_errors(root.get("data"))
    all_typed, line_errors = parse_res if parse_res is not None else (False, [])
    line_error_details = format_partial_write_details(root, all_typed, line_errors)
    if line_error_details:
        details_str = "".join(f"\n\t{detail}" for detail in line_error_details)
        reason = f"{reason}:{details_str}"
    return InfluxDBPartialWriteError(response, reason, line_errors)


def parse_typed_partial_write_object_or_none(data) -> Optional[InfluxDBPartialWriteLineError]:
    try:
        return parse_partial_write_data_item(data)
    except ValueError:
        return None


def format_line_error(line_error: InfluxDBPartialWriteLineError) -> str:
    if line_error.line_number is not None and line_error.original_line is not None:
        return f"line {line_error.line_number}: {line_error.error_message} ({line_error.original_line})"
    if line_error.line_number is not None:
        return f"line {line_error.line_number}: {line_error.error_message}"
    return f"{line_error.error_message}"


def parse_partial_write_data_item(item: Any) -> Optional[InfluxDBPartialWriteLineError]:
    if not isinstance(item, dict):
        return None

    line_number = item.get("line_number")
    if line_number is not None and (not isinstance(line_number, int) or isinstance(line_number, bool)):
        return None

    error_message = item.get("error_message")
    if not error_message:
        return None

    return InfluxDBPartialWriteLineError(line_number, error_message, item.get("original_line"))


def parse_partial_write_line_errors(data: Any) -> Optional[Tuple[bool, List[InfluxDBPartialWriteLineError]]]:
    if not isinstance(data, list):
        return None

    parsed_items = [parse_partial_write_data_item(item) for item in data]
    all_typed = all(item is not None for item in parsed_items)
    line_errors = [item for item in parsed_items if item is not None]
    return all_typed, line_errors


def extract_fallback_reason(response) -> str:
    # Fallback to header
    for header_key in ["X-Platform-Error-Code", "X-Influx-Error", "X-InfluxDb-Error"]:
        header_value = response.getheader(header_key)
        if header_value is not None:
            return header_value

    # Fallback to raw body
    if response.data is not None and response.data != "":
        return response.data

    # Fallback to http Status
    return response.reason


def format_partial_write_details(
        root: dict, all_typed: bool, line_errors: List[InfluxDBPartialWriteLineError]
) -> List[str]:
    if all_typed:
        return [format_line_error(err) for err in line_errors]

    return [
        json.dumps(raw, separators=(',', ':'))
        for raw in (root.get('data') or [])
        if raw is not None and raw != "null"
    ]
