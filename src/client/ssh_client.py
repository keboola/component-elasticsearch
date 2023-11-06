from io import StringIO
import paramiko


class SSHClient:
    def __init__(self, host, port, username, private_key):
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

        self.ssh_client.connect(
            self.host,
            port=self.port,
            username=self.username,
            pkey=private_key
        )

    def setup_tunnel(self, remote_host, remote_port, local_port):
        if not self.ssh_client:
            raise RuntimeError("SSH connection is not established")

        self.tunnel = self.ssh_client.get_transport().open_channel(
            'direct-tcpip',
            (remote_host, remote_port),
            ('127.0.0.1', local_port)
        )

    def close(self):
        if self.ssh_client:
            self.ssh_client.close()
        if self.tunnel:
            self.tunnel.close()
