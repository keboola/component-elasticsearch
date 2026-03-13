import logging
import select
import socketserver
import threading

import paramiko

logger = logging.getLogger(__name__)


class SshTunnelError(Exception):
    pass


class SshTunnel:
    """Pure-paramiko local port forwarder, replacing the unmaintained sshtunnel package."""

    def __init__(
        self,
        ssh_host: str,
        ssh_port: int,
        ssh_username: str,
        ssh_pkey: paramiko.PKey,
        remote_host: str,
        remote_port: int,
        local_host: str = "127.0.0.1",
        local_port: int = 0,
    ):
        self._ssh_host = ssh_host
        self._ssh_port = ssh_port
        self._ssh_username = ssh_username
        self._ssh_pkey = ssh_pkey
        self._remote_host = remote_host
        self._remote_port = remote_port
        self._local_host = local_host
        self._local_port = local_port
        self._client: paramiko.SSHClient | None = None
        self._server: socketserver.ThreadingTCPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def is_active(self) -> bool:
        return self._server is not None and self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self._client.connect(
                hostname=self._ssh_host,
                port=self._ssh_port,
                username=self._ssh_username,
                pkey=self._ssh_pkey,
                allow_agent=False,
                look_for_keys=False,
            )
        except Exception as e:
            raise SshTunnelError(f"SSH connection failed: {e}") from e

        transport = self._client.get_transport()

        class Handler(socketserver.BaseRequestHandler):
            def handle(inner_self):
                chan = transport.open_channel(
                    "direct-tcpip",
                    (self._remote_host, self._remote_port),
                    inner_self.request.getpeername(),
                )
                if chan is None:
                    return
                while True:
                    r, _, _ = select.select([inner_self.request, chan], [], [], 1.0)
                    if inner_self.request in r:
                        data = inner_self.request.recv(8192)
                        if not data:
                            break
                        chan.sendall(data)
                    if chan in r:
                        data = chan.recv(8192)
                        if not data:
                            break
                        inner_self.request.sendall(data)
                chan.close()

        self._server = socketserver.ThreadingTCPServer((self._local_host, self._local_port), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info(
            "SSH tunnel started: %s:%d -> %s:%d",
            self._local_host,
            self._server.server_address[1],
            self._remote_host,
            self._remote_port,
        )

    def stop(self) -> None:
        if self._server:
            self._server.shutdown()
            self._server = None
        if self._client:
            self._client.close()
            self._client = None
        self._thread = None
