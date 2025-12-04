"""
HTTP/2 proxy for capturing gRPC headers in tests.

This module provides a lightweight HTTP/2 proxy that supports both:
- h2c (HTTP/2 cleartext with prior knowledge) - which mitmproxy does not support
- h2 (HTTP/2 over TLS) - with runtime-generated self-signed certificates

It uses the hyper-h2 library to parse HTTP/2 frames and capture request/response headers.

Usage (h2c - cleartext):
    with H2HeaderProxy(upstream_host='127.0.0.1', upstream_port=8181) as proxy:
        client = InfluxDBClient3(
            host=proxy.url,
            token='...',
            database='...'
        )
        client.query("SELECT 1")
        assert proxy.get_request_header('grpc-accept-encoding') == 'identity, deflate, gzip'

Usage (h2 - TLS):
    with H2HeaderProxy(upstream_host='cloud.influxdata.com', upstream_port=443,
                       tls=True, upstream_tls=True) as proxy:
        client = InfluxDBClient3(
            host=proxy.url,
            token='...',
            database='...',
            verify_ssl=False  # Accept proxy's self-signed cert
        )
        client.query("SELECT 1")
        assert proxy.get_request_header('grpc-accept-encoding') == 'identity, deflate, gzip'
"""

import datetime
import ipaddress
import socket
import ssl
import threading
import select
import tempfile
import os

import h2.connection
import h2.config
import h2.events
import h2.exceptions

from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa


def _find_free_port():
    """Find a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        s.listen(1)
        return s.getsockname()[1]


def _generate_self_signed_cert():
    """Generate a self-signed certificate and private key in memory."""
    # Generate RSA key
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    # Generate certificate
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
    ])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
        .not_valid_after(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName("localhost"),
                x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
            ]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )

    # Serialize to PEM format
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return cert_pem, key_pem


class H2HeaderProxy:
    """
    HTTP/2 proxy that captures request and response headers.

    This proxy supports both:
    - h2c (HTTP/2 cleartext with prior knowledge) - for HTTP endpoints
    - h2 (HTTP/2 over TLS) - for HTTPS endpoints

    For TLS mode, generates a self-signed certificate at runtime.
    Use verify_ssl=False on the client to accept the self-signed cert.

    Attributes:
        port: The port the proxy is listening on
        captured: Dict with 'request' and 'response' lists of captured headers
        tls: Whether the proxy accepts TLS connections from clients
        upstream_tls: Whether the proxy uses TLS to connect to upstream
    """

    def __init__(self, upstream_host='127.0.0.1', upstream_port=8181, listen_port=None,
                 tls=False, upstream_tls=False):
        """
        Initialize the HTTP/2 proxy.

        Args:
            upstream_host: The upstream server hostname
            upstream_port: The upstream server port
            listen_port: Port to listen on (auto-assigned if None)
            tls: Accept TLS connections from clients (generates self-signed cert)
            upstream_tls: Use TLS when connecting to upstream server
        """
        self.upstream = (upstream_host, upstream_port)
        self.upstream_host = upstream_host
        self.port = listen_port or _find_free_port()
        self.captured = {'request': [], 'response': []}
        self.tls = tls
        self.upstream_tls = upstream_tls
        self._server_sock = None
        self._thread = None
        self._running = False
        self._ssl_context = None
        self._cert_file = None
        self._key_file = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()

    def start(self):
        """Start the proxy server."""
        # Set up TLS if enabled
        if self.tls:
            self._setup_tls()

        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind(('127.0.0.1', self.port))
        self._server_sock.listen(5)
        self._server_sock.settimeout(1.0)
        self._running = True

        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def _setup_tls(self):
        """Set up TLS context with a self-signed certificate."""
        # Generate self-signed certificate
        cert_pem, key_pem = _generate_self_signed_cert()

        # Write cert and key to temporary files (ssl.SSLContext needs files)
        self._cert_file = tempfile.NamedTemporaryFile(mode='wb', suffix='.pem', delete=False)
        self._cert_file.write(cert_pem)
        self._cert_file.close()

        self._key_file = tempfile.NamedTemporaryFile(mode='wb', suffix='.pem', delete=False)
        self._key_file.write(key_pem)
        self._key_file.close()

        # Create server SSL context
        self._ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self._ssl_context.set_alpn_protocols(['h2'])
        self._ssl_context.load_cert_chain(self._cert_file.name, self._key_file.name)

    def stop(self):
        """Stop the proxy server."""
        self._running = False
        if self._server_sock:
            self._server_sock.close()
            self._server_sock = None

        # Clean up temp certificate files
        if self._cert_file:
            try:
                os.unlink(self._cert_file.name)
            except OSError:
                pass
            self._cert_file = None
        if self._key_file:
            try:
                os.unlink(self._key_file.name)
            except OSError:
                pass
            self._key_file = None

    def clear(self):
        """Clear captured headers."""
        self.captured = {'request': [], 'response': []}

    def _accept_loop(self):
        """Accept and handle incoming connections."""
        while self._running:
            try:
                client_sock, _ = self._server_sock.accept()

                # Wrap with TLS if enabled
                if self.tls and self._ssl_context:
                    try:
                        client_sock = self._ssl_context.wrap_socket(
                            client_sock, server_side=True
                        )
                    except ssl.SSLError:
                        client_sock.close()
                        continue

                # Handle each connection in a new thread
                threading.Thread(
                    target=self._handle_connection,
                    args=(client_sock,),
                    daemon=True
                ).start()
            except socket.timeout:
                continue
            except OSError:
                break

    def _handle_connection(self, client_sock):
        """Handle a single client connection."""
        upstream_sock = None
        try:
            # Connect to upstream
            upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            upstream_sock.connect(self.upstream)

            # Wrap upstream with TLS if enabled
            if self.upstream_tls:
                upstream_ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                upstream_ssl_ctx.minimum_version = ssl.TLSVersion.TLSv1_2
                upstream_ssl_ctx.set_alpn_protocols(['h2'])
                upstream_ssl_ctx.check_hostname = False
                upstream_ssl_ctx.verify_mode = ssl.CERT_NONE
                upstream_sock = upstream_ssl_ctx.wrap_socket(
                    upstream_sock, server_hostname=self.upstream_host
                )

            # Create h2 connection state machines
            client_h2 = h2.connection.H2Connection(
                config=h2.config.H2Configuration(client_side=False)
            )
            server_h2 = h2.connection.H2Connection(
                config=h2.config.H2Configuration(client_side=True)
            )

            # Initialize connections
            client_h2.initiate_connection()
            client_sock.sendall(client_h2.data_to_send())
            server_h2.initiate_connection()
            upstream_sock.sendall(server_h2.data_to_send())

            client_sock.setblocking(False)
            upstream_sock.setblocking(False)

            self._proxy_loop(client_sock, upstream_sock, client_h2, server_h2)

        except Exception:
            pass  # Connection errors are expected when client disconnects
        finally:
            if client_sock:
                try:
                    client_sock.close()
                except Exception:
                    pass
            if upstream_sock:
                try:
                    upstream_sock.close()
                except Exception:
                    pass

    def _proxy_loop(self, client_sock, upstream_sock, client_h2, server_h2):
        """Main proxy loop - forward data between client and upstream."""

        def safe_end_stream(h2conn, stream_id):
            """End stream, ignoring errors if already closed."""
            try:
                h2conn.end_stream(stream_id)
            except h2.exceptions.StreamClosedError:
                pass

        for _ in range(1000):  # Max iterations to prevent infinite loop
            readable, _, _ = select.select([client_sock, upstream_sock], [], [], 0.05)

            for sock in readable:
                try:
                    data = sock.recv(65535)
                except BlockingIOError:
                    continue
                except ssl.SSLWantReadError:
                    continue
                except ssl.SSLWantWriteError:
                    continue

                if not data:
                    return

                if sock == client_sock:
                    # Client -> Proxy -> Upstream
                    events = client_h2.receive_data(data)
                    for ev in events:
                        if isinstance(ev, h2.events.RequestReceived):
                            hdrs = dict(ev.headers)
                            self.captured['request'].append(hdrs)
                            # Rewrite :authority and :scheme for upstream
                            fwd_headers = []
                            for k, v in ev.headers:
                                if k in (b':authority', ':authority'):
                                    # Use upstream host with port if not standard
                                    if self.upstream_tls and self.upstream[1] == 443:
                                        v = self.upstream_host.encode() if isinstance(k, bytes) else self.upstream_host
                                    elif not self.upstream_tls and self.upstream[1] == 80:
                                        v = self.upstream_host.encode() if isinstance(k, bytes) else self.upstream_host
                                    else:
                                        v = f"{self.upstream_host}:{self.upstream[1]}"
                                        v = v.encode() if isinstance(k, bytes) else v
                                elif k in (b':scheme', ':scheme') and self.upstream_tls:
                                    v = b'https' if isinstance(k, bytes) else 'https'
                                fwd_headers.append((k, v))
                            server_h2.send_headers(ev.stream_id, fwd_headers)
                        elif isinstance(ev, h2.events.DataReceived):
                            server_h2.send_data(ev.stream_id, ev.data)
                            client_h2.acknowledge_received_data(len(ev.data), ev.stream_id)
                        elif isinstance(ev, h2.events.StreamEnded):
                            safe_end_stream(server_h2, ev.stream_id)

                    to_send = server_h2.data_to_send()
                    if to_send:
                        upstream_sock.sendall(to_send)
                    to_send = client_h2.data_to_send()
                    if to_send:
                        client_sock.sendall(to_send)

                else:
                    # Upstream -> Proxy -> Client
                    events = server_h2.receive_data(data)

                    # Detect trailers-only response (ResponseReceived + StreamEnded, no data)
                    # This happens when server sends HEADERS with END_STREAM
                    stream_events = {}
                    for ev in events:
                        sid = getattr(ev, 'stream_id', None)
                        if sid is not None:
                            if sid not in stream_events:
                                stream_events[sid] = []
                            stream_events[sid].append(ev)

                    for ev in events:
                        if isinstance(ev, h2.events.ResponseReceived):
                            hdrs = dict(ev.headers)
                            self.captured['response'].append(hdrs)
                            # Check if this is a trailers-only response
                            stream_evs = stream_events.get(ev.stream_id, [])
                            has_stream_ended = any(isinstance(e, h2.events.StreamEnded) for e in stream_evs)
                            has_data = any(isinstance(e, h2.events.DataReceived) for e in stream_evs)
                            if has_stream_ended and not has_data:
                                # Trailers-only: send headers with END_STREAM
                                client_h2.send_headers(ev.stream_id, ev.headers, end_stream=True)
                            else:
                                client_h2.send_headers(ev.stream_id, ev.headers)
                        elif isinstance(ev, h2.events.DataReceived):
                            client_h2.send_data(ev.stream_id, ev.data)
                            server_h2.acknowledge_received_data(len(ev.data), ev.stream_id)
                        elif isinstance(ev, h2.events.StreamEnded):
                            # Only end stream if we didn't already (trailers-only case)
                            stream_evs = stream_events.get(ev.stream_id, [])
                            has_response = any(isinstance(e, h2.events.ResponseReceived) for e in stream_evs)
                            has_data = any(isinstance(e, h2.events.DataReceived) for e in stream_evs)
                            if not (has_response and not has_data):
                                # Normal case: end stream separately
                                safe_end_stream(client_h2, ev.stream_id)
                        elif isinstance(ev, h2.events.TrailersReceived):
                            hdrs = dict(ev.headers)
                            self.captured['response'].append(hdrs)
                            try:
                                client_h2.send_headers(ev.stream_id, ev.headers, end_stream=True)
                            except h2.exceptions.StreamClosedError:
                                pass

                    to_send = client_h2.data_to_send()
                    if to_send:
                        client_sock.sendall(to_send)
                    to_send = server_h2.data_to_send()
                    if to_send:
                        upstream_sock.sendall(to_send)

    def get_last_request_header(self, name):
        """
        Get a header value from the last captured request.

        Args:
            name: Header name (case-sensitive, typically lowercase)

        Returns:
            Header value as string, or None if not found
        """
        for hdrs in reversed(self.captured['request']):
            # Try both bytes and string keys
            for key in [name.encode() if isinstance(name, str) else name, name]:
                if key in hdrs:
                    v = hdrs[key]
                    return v.decode() if isinstance(v, bytes) else v
        return None

    def get_last_response_header(self, name):
        """
        Get a header value from the last captured response.

        Args:
            name: Header name (case-sensitive, typically lowercase)

        Returns:
            Header value as string, or None if not found
        """
        for hdrs in reversed(self.captured['response']):
            for key in [name.encode() if isinstance(name, str) else name, name]:
                if key in hdrs:
                    v = hdrs[key]
                    return v.decode() if isinstance(v, bytes) else v
        return None

    @property
    def url(self):
        """Get the proxy URL for client configuration."""
        scheme = "https" if self.tls else "http"
        return f"{scheme}://127.0.0.1:{self.port}"
