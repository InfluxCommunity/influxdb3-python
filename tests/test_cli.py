import io
import json
from argparse import Namespace
from unittest.mock import MagicMock, patch

import pyarrow as pa

from influxdb_client_3.cli import _run_query, build_parser, main
from influxdb_client_3.exceptions import InfluxDB3ClientQueryError


def _args(**overrides):
    base = dict(
        command="query",
        query=None,
        file_path=None,
        host=None,
        database=None,
        token=None,
        language="sql",
        output_format="json",
        output_file_path=None,
        query_timeout=None,
    )
    return Namespace(**{**base, **overrides})


def _mock_client(return_value=None, side_effect=None):
    client = MagicMock()
    client.__enter__.return_value = client
    if side_effect is not None:
        client.query.side_effect = side_effect
    else:
        client.query.return_value = return_value
    return client


def test_build_parser_query_alias():
    parser = build_parser()
    args = parser.parse_args(["q", "SELECT 1"])
    assert args.command == "q"
    assert args.query == "SELECT 1"


def test_run_query_uses_env_precedence():
    args = _args(query="SELECT 1")
    env = {
        "INFLUXDB3_HOST_URL": "http://from-primary:8181",
        "INFLUX_HOST": "http://from-legacy:8181",
        "INFLUXDB3_DATABASE_NAME": "db_primary",
        "INFLUX_DATABASE": "db_legacy",
        "INFLUXDB3_AUTH_TOKEN": "token_primary",
        "INFLUX_TOKEN": "token_legacy",
    }
    mock_client = _mock_client(return_value=pa.Table.from_pylist([{"value": 1}]))

    stdout, stderr = io.StringIO(), io.StringIO()

    with patch("influxdb_client_3.cli.InfluxDBClient3", return_value=mock_client) as client_ctor:
        rc = _run_query(args, stdout, stderr, env=env)

    assert rc == 0
    client_ctor.assert_called_once_with(
        host="http://from-primary:8181",
        database="db_primary",
        token="token_primary",
    )
    mock_client.query.assert_called_once_with(
        query="SELECT 1",
        language="sql",
        mode="all",
        database="db_primary",
    )


def test_run_query_formats_jsonl():
    args = _args(
        query="SELECT * FROM m",
        host="http://localhost:8181",
        database="db1",
        output_format="jsonl",
    )
    mock_client = _mock_client(return_value=pa.Table.from_pylist([{"a": 1}, {"a": 2}]))

    stdout, stderr = io.StringIO(), io.StringIO()

    with patch("influxdb_client_3.cli.InfluxDBClient3", return_value=mock_client):
        rc = _run_query(args, stdout, stderr, env={})

    assert rc == 0
    assert stdout.getvalue() == '{"a": 1}\n{"a": 2}\n'
    assert stderr.getvalue() == ""


def test_run_query_reads_query_from_file(tmp_path):
    query_file = tmp_path / "query.sql"
    query_file.write_text("SELECT * FROM cpu LIMIT 1", encoding="utf-8")

    args = _args(
        file_path=str(query_file),
        host="http://localhost:8181",
        database="db1",
    )
    mock_client = _mock_client(return_value=pa.Table.from_pylist([{"ok": True}]))

    stdout, stderr = io.StringIO(), io.StringIO()

    with patch("influxdb_client_3.cli.InfluxDBClient3", return_value=mock_client):
        rc = _run_query(args, stdout, stderr, env={})

    assert rc == 0
    mock_client.query.assert_called_once_with(
        query="SELECT * FROM cpu LIMIT 1",
        language="sql",
        mode="all",
        database="db1",
    )


def test_run_query_writes_json_error_for_query_exception():
    args = _args(query="SELECT bad", host="http://localhost:8181", database="db1")
    mock_client = _mock_client(side_effect=InfluxDB3ClientQueryError("bad query"))

    stdout, stderr = io.StringIO(), io.StringIO()

    with patch("influxdb_client_3.cli.InfluxDBClient3", return_value=mock_client):
        rc = _run_query(args, stdout, stderr, env={})

    assert rc == 1
    assert json.loads(stderr.getvalue()) == {"error": "bad query"}


def test_main_returns_1_when_database_missing():
    with patch("influxdb_client_3.cli.sys.stdout", new=io.StringIO()), patch(
        "influxdb_client_3.cli.sys.stderr", new=io.StringIO()
    ) as stderr:
        rc = main(["query", "SELECT 1"])

    assert rc == 1
    assert "Database is required" in stderr.getvalue()
