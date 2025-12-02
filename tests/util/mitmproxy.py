"""mitmproxy helper for intercepting HTTP/2 gRPC traffic in tests."""

import asyncio
import socket
import threading
import time


class HeaderCapture:
    """Addon that captures request/response headers from HTTP flows."""

    def __init__(self):
        self.requests = []  # List of (url, headers_dict)
        self.responses = []  # List of (url, headers_dict)

    def request(self, flow):
        """Called when a request is received."""
        self.requests.append((
            flow.request.pretty_url,
            dict(flow.request.headers)
        ))

    def response(self, flow):
        """Called when a response is received."""
        self.responses.append((
            flow.request.pretty_url,
            dict(flow.response.headers)
        ))

    def clear(self):
        """Clear captured headers."""
        self.requests.clear()
        self.responses.clear()

    def get_last_request_header(self, header_name):
        """Get a specific header from the last request."""
        if not self.requests:
            return None
        return self.requests[-1][1].get(header_name)

    def get_last_response_header(self, header_name):
        """Get a specific header from the last response."""
        if not self.responses:
            return None
        return self.responses[-1][1].get(header_name)


def _find_free_port():
    """Find a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port


class MitmproxyServer:
    """
    Context manager for running mitmproxy in a background thread.

    Intercepts HTTP/HTTPS traffic and captures headers for verification.
    Automatically selects a free port.

    Example usage:
        with MitmproxyServer() as proxy:
            client = InfluxDBClient3(
                host="...",
                proxy=proxy.url,
                verify_ssl=False
            )
            client.query("SELECT 1")

            # Check captured headers
            assert 'gzip' in proxy.capture.get_last_request_header('grpc-accept-encoding')
    """

    def __init__(self):
        self.port = None
        self.capture = HeaderCapture()
        self._master = None
        self._thread = None
        self._started = threading.Event()

    def __enter__(self):
        self.port = _find_free_port()

        def run_proxy():
            from mitmproxy.options import Options
            from mitmproxy.tools.dump import DumpMaster

            async def start_proxy():
                options = Options(
                    listen_host='127.0.0.1',
                    listen_port=self.port,
                    ssl_insecure=True,  # Don't verify upstream SSL certificates
                )

                self._master = DumpMaster(options, with_termlog=False, with_dumper=False)
                self._master.addons.add(self.capture)

                self._started.set()
                await self._master.run()

            asyncio.run(start_proxy())

        self._thread = threading.Thread(target=run_proxy, daemon=True)
        self._thread.start()

        # Wait for proxy to be ready
        self._started.wait(timeout=5.0)
        time.sleep(0.3)  # Additional wait for socket to be listening

        return self

    def __exit__(self, *args):
        if self._master:
            self._master.shutdown()

    @property
    def url(self):
        """Get the proxy URL for client configuration."""
        return f"http://127.0.0.1:{self.port}"
