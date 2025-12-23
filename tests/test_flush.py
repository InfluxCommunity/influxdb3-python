"""Tests for the flush() method in InfluxDBClient3 and WriteApi."""
import unittest
from unittest.mock import MagicMock, patch

from influxdb_client_3 import InfluxDBClient3, WriteOptions, write_client_options, WriteType


class TestFlushMethod(unittest.TestCase):
    """Test cases for the flush() method."""

    def test_flush_sends_buffered_data_and_allows_continued_writes(self):
        """Test that flush() sends pending data and allows continued writes."""
        write_count = 0

        def success_callback(conf, data):
            nonlocal write_count
            write_count += 1

        write_options = WriteOptions(
            write_type=WriteType.batching,
            batch_size=1000,
            flush_interval=60_000,
            max_close_wait=5_000
        )

        wc_opts = write_client_options(
            success_callback=success_callback,
            write_options=write_options
        )

        with patch('influxdb_client_3.write_client.client.write_api.WriteApi._post_write') as mock_post:
            mock_post.return_value = MagicMock()

            client = InfluxDBClient3(
                host="http://localhost:8086",
                token="my-token",
                database="my-db",
                write_client_options=wc_opts
            )

            try:
                # Write data, flush, write more, flush again
                for i in range(5):
                    client.write(f"test,tag=value field={i}i")
                client.flush()

                for i in range(5):
                    client.write(f"test,tag=value field={i}i")
                client.flush()

                # Both batches should have been flushed
                self.assertEqual(2, write_count)
                self.assertEqual(2, mock_post.call_count)

                # Verify that all 10 data points (5 per batch) were sent
                for call in mock_post.call_args_list:
                    args, kwargs = call
                    body = kwargs.get('body') or args[3]
                    if isinstance(body, bytes):
                        body = body.decode('utf-8')
                    for i in range(5):
                        self.assertIn(f"test,tag=value field={i}i", body)
            finally:
                client.close()

    def test_flush_is_safe_in_synchronous_mode_and_after_close(self):
        """Test that flush() doesn't crash in sync mode or after close."""
        # Test synchronous mode
        sync_opts = write_client_options(write_options=WriteOptions(write_type=WriteType.synchronous))
        with patch('influxdb_client_3.write_client.client.write_api.WriteApi._post_write'):
            client = InfluxDBClient3(host="http://localhost:8086", token="t", database="db",
                                     write_client_options=sync_opts)
            client.flush()  # Should not raise
            client.close()

        # Test flush after close in batching mode
        batch_opts = write_client_options(write_options=WriteOptions(write_type=WriteType.batching))
        with patch('influxdb_client_3.write_client.client.write_api.WriteApi._post_write'):
            client = InfluxDBClient3(host="http://localhost:8086", token="t", database="db",
                                     write_client_options=batch_opts)
            client.close()
            client.flush()  # Should not raise


if __name__ == '__main__':
    unittest.main()
