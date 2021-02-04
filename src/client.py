import json
import logging
import paramiko
import io
import socket

from furl import furl
from typing import List, Tuple

Headers = List[Tuple[str, str]]

# Workaround for re-key timeout: https://github.com/paramiko/paramiko/issues/822
paramiko.packet.Packetizer.REKEY_BYTES = 10000000000


class SshClient:

    REQUEST_SIZE = 2000
    DEFAULT_SCROLL = '5m'

    def __init__(self, SshTunnel, Database):

        pkey_file = io.StringIO(SshTunnel.key)
        pkey = self._parse_private_key(pkey_file)

        # Set up SSH paramiko client
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.ssh.connect(hostname=SshTunnel.hostname, port=SshTunnel.port,
                             username=SshTunnel.username, pkey=pkey)
        except (socket.gaierror, paramiko.ssh_exception.AuthenticationException):
            logging.exception("Could not establish SSH tunnel. Check that all SSH parameters are correct.")
            raise

        self.db = Database

    def _parse_private_key(self, keyfile):
        # try all versions of encryption keys
        pkey = None
        failed = False
        try:
            pkey = paramiko.RSAKey.from_private_key(keyfile)
        except paramiko.SSHException:
            logging.warning("RSS Private key invalid, trying DSS.")
            failed = True
        except IndexError:
            logging.exception("Could not read RSS Private Key - have you provided it correctly?")
            raise
        # DSS
        if failed:
            try:
                pkey = paramiko.DSSKey.from_private_key(keyfile)
                failed = False
            except paramiko.SSHException:
                logging.warning("DSS Private key invalid, trying ECDSAKey.")
                failed = True
        # ECDSAKey
        if failed:
            try:
                pkey = paramiko.ECDSAKey.from_private_key(keyfile)
                failed = False
            except paramiko.SSHException:
                logging.warning("ECDSAKey Private key invalid, trying Ed25519Key.")
                failed = True
        # Ed25519Key
        if failed:
            try:
                pkey = paramiko.Ed25519Key.from_private_key(keyfile)
            except paramiko.SSHException as e:
                logging.warning("Ed25519Key Private key invalid.")
                raise e

        return pkey

    def build_curl(self, url, request_type, headers=[], json_body=None):

        # Start of curl string
        curl = 'curl -i'
        curl += f' --request {request_type}'

        # Add headers
        _header_string = ''
        for header in headers:
            _header_string += f' -H "{header[0]}: {header[1]}"'

        curl += _header_string

        # Add JSON body
        if json_body is not None:
            curl += f' --data \'{json.dumps(json_body)}\''

        curl += f' {url}'

        logging.debug(f"Constructed cURL: {curl}.")
        return curl

    def get_first_page(self, index, parameters):

        db_url = furl(f'{self.db.host}:{self.db.port}')
        db_url /= f'{index}/_search'
        db_url.args['scroll'] = self.DEFAULT_SCROLL

        parameters['size'] = self.REQUEST_SIZE

        curl = self.build_curl(db_url, 'POST', [('Content-Type', 'application/json')], parameters)

        _, stdout, stderr = self.ssh.exec_command(curl)
        out, err = stdout.read(), stderr.read()

        return out.decode().strip(), err.decode().strip()

    def get_scroll(self, scroll_id):

        db_url = furl(f'{self.db.host}:{self.db.port}')
        db_url /= '_search/scroll'

        data = {'scroll': self.DEFAULT_SCROLL, 'scroll_id': scroll_id}

        curl = self.build_curl(db_url, 'POST', [('Content-Type', 'application/json')], data)

        _, stdout, stderr = self.ssh.exec_command(curl)
        out, err = stdout.read(), stderr.read()

        return out.decode().strip(), err.decode().strip()
