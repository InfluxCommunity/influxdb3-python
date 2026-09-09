import logging
import queue
from unittest.mock import Mock

import pytest

from influxdb_client_3.write_client.client.util.multiprocessing_helper import MultiprocessingWriter, _PoisonPill


def make_writer(**kwargs):
    return MultiprocessingWriter(
        start_method="fork",
        host="http://localhost:8086",
        database="test",
        rest_client=Mock(),
        **kwargs,
    )


class RecordingWriteApi:
    def __init__(self, error=None, close_error=None):
        self.error = error
        self.close_error = close_error
        self.writes = []
        self.close_calls = 0

    def write(self, **record):
        self.writes.append(record)
        if self.error is not None:
            raise self.error

    def close(self):
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


class FakeProcess:
    def __init__(self):
        self.start_calls = 0
        self.join_calls = 0

    def start(self):
        self.start_calls += 1

    def join(self, timeout=None):
        self.join_calls += 1

    def is_alive(self):
        return False


class HangingProcess(FakeProcess):
    def __init__(self):
        super().__init__()
        self.join_timeouts = []
        self.terminate_calls = 0

    def join(self, timeout=None):
        self.join_timeouts.append(timeout)

    def is_alive(self):
        return True

    def terminate(self):
        self.terminate_calls += 1


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


def test_write_after_start_queues_record():
    writer = make_writer()
    writer.process = FakeProcess()
    writer.queue_ = FakeQueue()
    writer.start()
    writer.write(record="value")

    assert writer.queue_.items == [{"record": "value"}]


def test_start_after_close_raises_runtime_error():
    writer = make_writer()
    writer.close()

    with pytest.raises(RuntimeError, match="after it has been closed"):
        writer.start()


def test_start_twice_raises_runtime_error():
    writer = make_writer()
    writer.process = FakeProcess()
    writer.queue_ = FakeQueue()
    writer.start()

    with pytest.raises(RuntimeError, match="already started"):
        writer.start()


def test_get_start_processing_method_returns_context_method():
    assert make_writer().get_start_processing_method() == "fork"


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


def test_worker_timeout_continues_when_write_api_close_fails():
    writer = make_writer()
    writer.queue_ = Mock()
    writer.queue_.get.side_effect = queue.Empty
    write_api = RecordingWriteApi(close_error=ValueError("close failed"))

    writer.run(write_api, writer.disposed, 0.01, None)

    assert writer.disposed.value == 1
    assert write_api.close_calls == 1


def test_worker_poison_pill_closes_write_api():
    writer = make_writer()
    writer.queue_.put(_PoisonPill())
    write_api = RecordingWriteApi()

    writer.run(write_api, writer.disposed, 0.01, None)
    writer.queue_.join()

    assert write_api.close_calls == 1


def test_shutdown_callback_is_called_once_when_invoked_repeatedly():
    writer = make_writer()
    callback = Mock()

    writer._call_on_shutdown(callback)
    writer._call_on_shutdown(callback)

    callback.assert_called_once_with()


def test_shutdown_callback_failure_is_logged(caplog):
    writer = make_writer()
    callback = Mock(side_effect=ValueError("callback failed"))

    writer._call_on_shutdown(callback)

    assert "shutdown callback failed" in caplog.text


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


def test_close_terminates_worker_after_join_timeout():
    writer = make_writer(close_timeout=0.01)
    writer.process = HangingProcess()
    writer.queue_ = FakeQueue()
    writer.start()
    writer.close()

    assert writer.process.join_timeouts == [0.01, 0.01]
    assert writer.process.terminate_calls == 1


def test_context_manager_uses_close():
    writer = make_writer()
    writer.start = Mock()
    writer.close = Mock()

    writer.__enter__()
    writer.__exit__(None, None, None)

    writer.start.assert_called_once_with()
    writer.close.assert_called_once_with()


def test_destructor_swallows_cleanup_failure(caplog):
    caplog.set_level(logging.DEBUG)
    writer = make_writer()
    writer.close = Mock(side_effect=RuntimeError("cleanup failed"))

    writer.__del__()

    assert "cleanup failed" in caplog.text
