import argparse
import csv
import io
import json
import os
import sys
from typing import Mapping, Optional

import pyarrow as pa

from influxdb_client_3 import (
    INFLUX_DATABASE,
    INFLUX_HOST,
    INFLUX_TOKEN,
    InfluxDBClient3,
)
from influxdb_client_3.exceptions import InfluxDB3ClientQueryError, InfluxDBError


def _resolve_option(
    cli_value: Optional[str],
    env: Mapping[str, str],
    primary_env: str,
    secondary_env: Optional[str] = None,
    default: Optional[str] = None,
) -> Optional[str]:
    if cli_value is not None:
        return cli_value

    for var in (primary_env, secondary_env):
        if not var:
            continue
        value = env.get(var)
        if value not in (None, ""):
            return value

    return default


def _rows_to_csv(rows, fieldnames):
    buff = io.StringIO()
    writer = csv.DictWriter(buff, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buff.getvalue()


def _rows_to_pretty(rows, fieldnames):
    if not rows:
        return "(0 rows)"

    widths = {name: len(name) for name in fieldnames}
    for row in rows:
        for name in fieldnames:
            widths[name] = max(widths[name], len(str(row.get(name, ""))))

    header = " | ".join(name.ljust(widths[name]) for name in fieldnames)
    sep = "-+-".join("-" * widths[name] for name in fieldnames)
    lines = [header, sep]
    for row in rows:
        lines.append(" | ".join(str(row.get(name, "")).ljust(widths[name]) for name in fieldnames))
    return "\n".join(lines)


def _rows_to_json(rows, fieldnames):
    return json.dumps(rows, default=str)


def _rows_to_jsonl(rows, fieldnames):
    if not rows:
        return ""
    return "\n".join(json.dumps(row, default=str) for row in rows)


_FORMATTERS = {
    "json": _rows_to_json,
    "jsonl": _rows_to_jsonl,
    "csv": _rows_to_csv,
    "pretty": _rows_to_pretty,
}


def _format_table(table: pa.Table, output_format: str) -> str:
    rows = table.to_pylist()
    fieldnames = table.schema.names
    return _FORMATTERS[output_format](rows, fieldnames)


def _ensure_trailing_nl(text: str) -> str:
    if not text:
        return ""
    return text if text.endswith("\n") else text + "\n"


def _write_error(stderr, message: str):
    stderr.write(json.dumps({"error": str(message)}) + "\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="influx3", description="InfluxDB 3 query CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    query_parser = subparsers.add_parser("query", aliases=["q"], help="Run a SQL or InfluxQL query")
    query_parser.add_argument("query", nargs="?", help="The query string to execute")
    query_parser.add_argument("-f", "--file", dest="file_path", help="File containing the query")
    query_parser.add_argument("-H", "--host", dest="host", help="InfluxDB host URL")
    query_parser.add_argument("-d", "--database", dest="database", help="Database name")
    query_parser.add_argument("--token", dest="token", help="Authentication token")
    query_parser.add_argument(
        "-l",
        "--language",
        dest="language",
        choices=["sql", "influxql"],
        default="sql",
        help="Query language",
    )
    query_parser.add_argument(
        "--format",
        dest="output_format",
        choices=list(_FORMATTERS),
        default="json",
        help="Output format",
    )
    query_parser.add_argument("-o", "--output", dest="output_file_path", help="Write output to file")
    query_parser.add_argument("--query-timeout", dest="query_timeout", type=int, help="Query timeout in ms")
    query_parser.set_defaults(func=_run_query)
    return parser


def _run_query(args, stdout, stderr, env: Optional[Mapping[str, str]] = None) -> int:
    if env is None:
        env = os.environ

    host = _resolve_option(args.host, env, "INFLUXDB3_HOST_URL", INFLUX_HOST, "http://127.0.0.1:8181")
    database = _resolve_option(args.database, env, "INFLUXDB3_DATABASE_NAME", INFLUX_DATABASE)
    token = _resolve_option(args.token, env, "INFLUXDB3_AUTH_TOKEN", INFLUX_TOKEN)

    if (args.query is None) == (args.file_path is None):
        _write_error(stderr, "Provide exactly one of query or --file.")
        return 1

    if not database:
        _write_error(stderr, "Database is required. Set --database or INFLUXDB3_DATABASE_NAME.")
        return 1

    if args.query_timeout is not None and args.query_timeout < 0:
        _write_error(stderr, "--query-timeout must be non-negative.")
        return 1

    try:
        query = args.query
        if args.file_path:
            with open(args.file_path, "r", encoding="utf-8") as file_handle:
                query = file_handle.read()

        query_kwargs = {}
        if args.query_timeout is not None:
            query_kwargs["query_timeout"] = args.query_timeout

        with InfluxDBClient3(host=host, database=database, token=token, **query_kwargs) as client:
            table = client.query(
                query=query,
                language=args.language,
                mode="all",
                database=database,
            )

        payload = _ensure_trailing_nl(_format_table(table, args.output_format))
        if args.output_file_path:
            with open(args.output_file_path, "w", encoding="utf-8", newline="") as file_handle:
                file_handle.write(payload)
        else:
            stdout.write(payload)
        return 0
    except (InfluxDB3ClientQueryError, InfluxDBError, OSError, pa.ArrowException) as error:
        _write_error(stderr, str(error))
        return 1


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args, sys.stdout, sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
