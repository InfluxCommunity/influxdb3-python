import queue
from unittest.mock import Mock

import pytest

from influxdb_client_3.write_client.client.util.multiprocessing_helper import MultiprocessingWriter


def make_writer(**kwargs):
    return MultiprocessingWriter(
        start_method="fork",
        host="http://localhost:8086",
        database="test",
        rest_client=Mock(),
        **kwargs,
    )


class RecordingWriteApi:
    def __init__(self, error=None):
        self.error = error
        self.writes = []
        self.close_calls = 0

    def write(self, **record):
        self.writes.append(record)
        if self.error is not None:
            raise self.error

    def close(self):
        self.close_calls += 1


class FakeProcess:
    def __init__(self):
        self.start_calls = 0
        self.join_calls = 0

    def start(self):
        self.start_calls += 1

    def join(self):
        self.join_calls += 1


class FakeQueue:
    def __init__(self):
        self.items = []

    def put(self, item):
        self.items.append(item)


def test_write_before_start_raises_runtime_error():
    writer = make_writer()

    with pytest.raises(RuntimeError, match="writer is not started"):
        writer.write(record="value")


def test_write_after_close_raises_runtime_error():
    writer = make_writer()
    writer.close()

    with pytest.raises(RuntimeError, match="writer is closed"):
        writer.write(record="value")


def test_worker_timeout_closes_api_and_invokes_callback_once():
    writer = make_writer(process_ttl=0.01)
    writer.queue_ = Mock()
    writer.queue_.get.side_effect = queue.Empty
    write_api = RecordingWriteApi()
    on_shutdown = Mock()

    writer.run(write_api, writer.disposed, 0.01, on_shutdown)
    writer.close()

    assert writer.disposed.value == 1
    assert write_api.close_calls == 1
    on_shutdown.assert_called_once_with()
    with pytest.raises(RuntimeError, match="writer is closed"):
        writer.write(record="after-timeout")


def test_worker_write_failure_marks_queue_item_done():
    writer = make_writer()
    writer.queue_.put({"record": "value"})
    write_api = RecordingWriteApi(error=ValueError("write failed"))

    writer.run(write_api, writer.disposed, 0.01, None)
    writer.queue_.join()

    assert write_api.writes == [{"record": "value"}]
    assert writer.disposed.value == 1


def test_close_is_idempotent_and_sends_one_poison_pill():
    writer = make_writer()
    writer.process = FakeProcess()
    writer.queue_ = FakeQueue()
    writer.start()
    writer.close()
    writer.close()

    assert writer.process.start_calls == 1
    assert writer.process.join_calls == 1
    assert len(writer.queue_.items) == 1


def test_context_manager_uses_close():
    writer = make_writer()
    writer.start = Mock()
    writer.close = Mock()

    writer.__enter__()
    writer.__exit__(None, None, None)

    writer.start.assert_called_once_with()
    writer.close.assert_called_once_with()
