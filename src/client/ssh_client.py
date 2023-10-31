from io import StringIO
import paramiko


class SSHClient:
    def __init__(self, host, port, username, private_key):
        self.host = host
        self.port = port
        self.username = username
        self.private_key = private_key
        self.ssh_client = None

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

    def close(self):
        if self.ssh_client:
            self.ssh_client.close()
