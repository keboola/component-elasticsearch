from io import StringIO
import paramiko
import logging


class SSHClient:
    def __init__(self, host, port, username, private_key):
        self.transport = None
        self.host = host
        self.port = port
        self.username = username
        self.private_key = private_key
        self.ssh_client = None
        self.tunnel = None

    def connect(self):
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.load_system_host_keys()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        private_key = paramiko.RSAKey(file_obj=StringIO(self.private_key))

        logging.info(f"Establishing SSH connection to {self.host}:{self.port}")

        self.ssh_client.connect(
            self.host,
            port=self.port,
            username=self.username,
            pkey=private_key
        )

    def setup_tunnel(self, remote_host, remote_port: int):
        if not self.ssh_client:
            raise RuntimeError("SSH connection is not established")

        transport = self.ssh_client.get_transport()

        logging.info(f"Tunel remote_host: {remote_host}, remote port: {remote_port}")
        transport.request_port_forward(remote_host, remote_port)

    def close(self):
        if self.ssh_client:
            self.ssh_client.close()
