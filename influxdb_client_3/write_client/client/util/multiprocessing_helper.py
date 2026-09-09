"""
Helpers classes to make easier use the client in multiprocessing environment.

For more information how the multiprocessing works see Python's
`reference docs <https://docs.python.org/3/library/multiprocessing.html>`_.
"""
import logging
import multiprocessing
import os
import queue

from influxdb_client_3 import write_client_options
from influxdb_client_3.exceptions import InfluxDBError
from influxdb_client_3.write_client import WriteOptions, WriteApi
from influxdb_client_3.write_client._sync import rest_client

logger = logging.getLogger('influxdb_client.client.util.multiprocessing_helper')


def _success_callback(conf: (str, str, str), data: str):
    """Successfully writen batch."""
    logger.debug(f"Written batch: {conf}, data: {data}")


def _error_callback(conf: (str, str, str), data: str, exception: InfluxDBError):
    """Unsuccessfully writen batch."""
    logger.debug(f"Cannot write batch: {conf}, data: {data} due: {exception}")


def _retry_callback(conf: (str, str, str), data: str, exception: InfluxDBError):
    """Retryable error."""
    logger.debug(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")


class _PoisonPill:
    """To notify process to terminate."""

    pass


class MultiprocessingWriter:
    """
    The Helper class to write data into InfluxDB in an independent OS process.

    Example:
        .. code-block:: python

            from influxdb_client import WriteOptions
            from influxdb_client.client.util.multiprocessing_helper import MultiprocessingWriter


            def main():
                writer = MultiprocessingWriter(url="http://localhost:8086", token="my-token", org="my-org",
                                               write_options=WriteOptions(batch_size=100))
                writer.start()

                for x in range(1, 1000):
                    writer.write(bucket="my-bucket", record=f"mem,tag=a value={x}i {x}")

                writer.__del__()


            if __name__ == '__main__':
                main()


    How to use with context_manager:
        .. code-block:: python

            from influxdb_client import WriteOptions
            from influxdb_client.client.util.multiprocessing_helper import MultiprocessingWriter


            def main():
                with MultiprocessingWriter(url="http://localhost:8086", token="my-token", org="my-org",
                                           write_options=WriteOptions(batch_size=100)) as writer:
                    for x in range(1, 1000):
                        writer.write(bucket="my-bucket", record=f"mem,tag=a value={x}i {x}")


            if __name__ == '__main__':
                main()


    How to handle batch events:
        .. code-block:: python

            from influxdb_client import WriteOptions
            from influxdb_client.client.exceptions import InfluxDBError
            from influxdb_client.client.util.multiprocessing_helper import MultiprocessingWriter


            class BatchingCallback(object):

                def success(self, conf: (str, str, str), data: str):
                    print(f"Written batch: {conf}, data: {data}")

                def error(self, conf: (str, str, str), data: str, exception: InfluxDBError):
                    print(f"Cannot write batch: {conf}, data: {data} due: {exception}")

                def retry(self, conf: (str, str, str), data: str, exception: InfluxDBError):
                    print(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")


            def main():
                callback = BatchingCallback()
                with MultiprocessingWriter(url="http://localhost:8086", token="my-token", org="my-org",
                                           success_callback=callback.success,
                                           error_callback=callback.error,
                                           retry_callback=callback.retry) as writer:

                    for x in range(1, 1000):
                        writer.write(bucket="my-bucket", record=f"mem,tag=a value={x}i {x}")


            if __name__ == '__main__':
                main()


    """

    def __init__(self,
                 start_method='spawn',
                 process_ttl=300,
                 on_shutdown=None,
                 close_timeout=60,
                 **kwargs
                 ) -> None:
        """
        Initialize defaults.

        For more information on how to initialize the writer, see the examples above.

        :param start_method: The method used to start the subprocess.
            See :func:`multiprocessing.get_context` for more information.
        :param process_ttl: The timeout in seconds for waiting for data in the underlying queue.
        :param on_shutdown: The callback function called when the worker process is shut down
               or when `MultiprocessingWriter` class start closing.
        :param close_timeout: The timeout in seconds for waiting for the worker to shut down gracefully.
        :param kwargs: Arguments are passed into the ``WriteApi`` and ``write_client_options``.
            Common arguments include: `host`, `token`, `database`, `org`, `write_options`, `success_callback`,
            `error_callback`, `retry_callback`, `default_header`, and `rest_client`.
        """

        wco = write_client_options(write_options=kwargs.get('write_options', WriteOptions()),
                                   success_callback=kwargs.get('success_callback', _success_callback),
                                   error_callback=kwargs.get('error_callback', _error_callback),
                                   retry_callback=kwargs.get('retry_callback', _retry_callback)
                                   )

        if kwargs.get('rest_client') is not None:
            rest = kwargs.get('rest_client')
        else:
            token = kwargs.get('token')
            default_header = {'Authorization': f'Token {token}'}
            rest = rest_client.RestClient(
                base_url=kwargs.get('host'),
                default_header=default_header,
            )

        write_api = WriteApi(
            bucket=kwargs.get('database'),
            org=kwargs.get('org'),
            default_header=kwargs.get('default_header'),
            rest_client=rest,
            **wco
        )

        self.ctx = multiprocessing.get_context(start_method)
        self.on_shutdown = on_shutdown
        self.close_timeout = close_timeout
        self.disposed = self.ctx.Value('i', 0)
        self._shutdown_called = self.ctx.Value('i', 0)
        self.__started__ = False
        self._closed = False
        self.process = self.ctx.Process(target=self.run, args=(write_api, self.disposed, process_ttl, self.on_shutdown))
        self.kwargs = kwargs
        self.queue_ = self.ctx.JoinableQueue()

    def write(self, **kwargs) -> None:
        """
        Append time-series data into the underlying queue.

        For more information on how to pass arguments, see the examples above.

        :param kwargs: arguments are passed into the `` write `` function of ``WriteApi``
        :return: None
        """
        if self.disposed.value != 0 or self._closed:
            raise RuntimeError('Cannot write data: the writer is closed.')
        if not self.__started__:
            raise RuntimeError('Cannot write data: the writer is not started.')
        self.queue_.put(kwargs)

    def _call_on_shutdown(self, callback=None) -> None:
        """Invoke the shutdown callback once across the parent and worker processes."""
        callback = self.on_shutdown if callback is None else callback
        if callback is None:
            return

        with self._shutdown_called.get_lock():
            if self._shutdown_called.value != 0:
                return
            self._shutdown_called.value = 1

        try:
            callback()
        except Exception:
            logger.exception("The multiprocessing writer shutdown callback failed")

    @staticmethod
    def _close_write_api(write_api: WriteApi) -> None:
        """Close the worker's WriteApi without preventing process shutdown."""
        try:
            write_api.close()
        except Exception:
            logger.exception("The multiprocessing writer failed to close the WriteApi")

    def run(self, write_api: WriteApi, disposed, process_ttl, on_shutdown) -> None:
        """
        The worker loop that consumes and writes data from the queue.

        This method is executed in a separate process. It continuously pulls records from the
        internal queue and writes them to InfluxDB using the provided ``WriteApi``.

        The loop terminates if:
            - A ``_PoisonPill`` is received (graceful shutdown).
            - The queue remains empty for longer than ``process_ttl`` seconds.

        :param write_api: The ``WriteApi`` instance used to perform the actual write operations.
        :param disposed: A ``multiprocessing.Value`` indicating if the writer has been disposed.
        :param process_ttl: The timeout in seconds to wait for new data before terminating the process.
        :param on_shutdown: The callback function called when the worker process is shut down.
        :return: None
        """

        # Infinite loop - until poison pill or `process_ttl`
        while True:
            try:
                next_record = self.queue_.get(timeout=process_ttl)
            except queue.Empty:
                if disposed.value == 0:
                    self._close_write_api(write_api)
                    disposed.value = 1
                    self._call_on_shutdown(on_shutdown)
                break

            try:
                if type(next_record) is _PoisonPill:
                    # Poison pill means break the loop
                    logger.info("flushing data...")
                    self._close_write_api(write_api)
                    logger.info("closed")
                    break

                try:
                    write_api.write(**next_record)
                except Exception:
                    logger.exception("The multiprocessing writer failed to write a record")
            finally:
                self.queue_.task_done()

    def start(self) -> None:
        """Start an independent process for writing data into InfluxDB."""
        if self._closed or self.disposed.value != 0:
            raise RuntimeError('Cannot start the writer after it has been closed.')
        if self.__started__:
            raise RuntimeError('The writer is already started.')
        self.process.start()
        self.__started__ = True

    def get_start_processing_method(self):
        return self.ctx.get_start_method()

    def __enter__(self):
        """Enter the runtime context related to this object."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object."""
        self.close()

    def close(self) -> None:
        """Flush queued writes and close the worker process once."""
        if self._closed:
            return

        self._closed = True
        is_worker_process = getattr(self.process, 'pid', None) == os.getpid()
        try:
            if self.__started__ and not is_worker_process:
                if self.disposed.value == 0:
                    self.queue_.put(_PoisonPill())
                self.process.join(timeout=self.close_timeout)
                if self.process.is_alive():
                    logger.warning("The multiprocessing writer worker did not shut down before the timeout")
                    self.process.terminate()
                    self.process.join(timeout=self.close_timeout)
        finally:
            self.__started__ = False
            self.disposed.value = 1
            if not is_worker_process:
                self._call_on_shutdown()

    def __del__(self):
        """Best-effort cleanup for writers that were not explicitly closed."""
        try:
            self.close()
        except Exception:
            logger.debug("The multiprocessing writer cleanup failed", exc_info=True)
