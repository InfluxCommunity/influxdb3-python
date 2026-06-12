"""
Helpers classes to make easier use the client in multiprocessing environment.

For more information how the multiprocessing works see Python's
`reference docs <https://docs.python.org/3/library/multiprocessing.html>`_.
"""
import logging
import multiprocessing
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

    __started__ = False

    def __init__(self,
                 start_method='spawn',
                 process_ttl=300,
                 on_shutdown=None,
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
        self.disposed = self.ctx.Value('i', 0)
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
        assert self.__started__ is True, 'Cannot write data: the writer is not started.'
        if self.disposed.value == 0:
            self.queue_.put(kwargs)
        else:
            raise Exception('Cannot write data: the writer is closed.')

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
                    write_api.close()
                    disposed.value = 1
                    if on_shutdown is not None:
                        on_shutdown()
                    break

            if type(next_record) is _PoisonPill:
                # Poison pill means break the loop
                logger.info("flushing data...")
                write_api.close()
                logger.info("closed")
                self.queue_.task_done()
                break
            write_api.write(**next_record)
            self.queue_.task_done()

    def start(self) -> None:
        """Start an independent process for writing data into InfluxDB."""
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
        self.__del__()

    def __del__(self):
        """Dispose of the client and write_api."""
        if self.__started__ and self.disposed.value == 0:
            self.queue_.put(_PoisonPill())
            self.queue_.join()
            self.process.join()
            self.queue_ = None
        self.__started__ = False
        self.disposed.value = 1
        if self.on_shutdown is not None:
            self.on_shutdown()
