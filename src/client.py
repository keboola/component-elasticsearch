import io
import json
import logging
import socket
import sys
from typing import List, Tuple

import paramiko
from furl import furl

Headers = List[Tuple[str, str]]

# Workaround for re-key timeout: https://github.com/paramiko/paramiko/issues/822
paramiko.packet.Packetizer.REKEY_BYTES = 1e12

SIZE_PARAM = 'size'
SCROLL_PARAM = 'scroll'

DEFAULT_SIZE = 2000
DEFAULT_SCROLL = '15m'


class SshClient:

    def __init__(self, SshTunnel, Database):

        pkey = self._parse_private_key(SshTunnel.key)

        # Set up SSH paramiko client
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.ssh.connect(hostname=SshTunnel.hostname, port=SshTunnel.port,
                             username=SshTunnel.username, pkey=pkey)
        except (socket.gaierror, paramiko.ssh_exception.AuthenticationException):
            logging.exception("Could not establish SSH tunnel. Check that all SSH parameters are correct.")
            sys.exit(1)

        self.db = Database

    def _parse_private_key(self, key):
        # try all versions of encryption keys
        pkey = None
        failed = False
        try:
            keyfile = io.StringIO(key)
            pkey = paramiko.RSAKey.from_private_key(keyfile)
        except paramiko.SSHException as e:
            logging.debug(e)
            logging.warning("RSS Private key invalid, trying DSS.")
            failed = True
        except IndexError:
            logging.exception("Could not read RSS Private Key - have you provided it correctly?")
            sys.exit(1)
        # DSS
        if failed:
            try:
                keyfile = io.StringIO(key)
                pkey = paramiko.DSSKey.from_private_key(keyfile)
                failed = False
            except paramiko.SSHException:
                logging.warning("DSS Private key invalid, trying ECDSAKey.")
                failed = True
            except IndexError:
                logging.exception("Could not read DSS Private Key - have you provided it correctly?")
                failed = True
        # ECDSAKey
        if failed:
            try:
                keyfile = io.StringIO(key)
                pkey = paramiko.ECDSAKey.from_private_key(keyfile)
                failed = False
            except paramiko.SSHException:
                logging.warning("ECDSAKey Private key invalid, trying Ed25519Key.")
                failed = True
            except IndexError:
                logging.exception("Could not read ECDSAKey Private Key - have you provided it correctly?")
                failed = True
        # Ed25519Key
        if failed:
            try:
                keyfile = io.StringIO(key)
                pkey = paramiko.Ed25519Key.from_private_key(keyfile)
            except paramiko.SSHException as e:
                logging.warning("Ed25519Key Private key invalid.")
                raise e
            except IndexError:
                logging.exception("Could not read Ed25519Key Private Key - have you provided it correctly?")
                sys.exit(1)

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

    def get_first_page(self, index, body):

        db_url = furl(f'{self.db.host}:{self.db.port}')
        db_url /= f'{index}/_search'

        self._default_scroll = body.pop(SCROLL_PARAM, DEFAULT_SCROLL)
        db_url.args[SCROLL_PARAM] = self._default_scroll

        if SIZE_PARAM in body:
            self._default_size = body[SIZE_PARAM]
        else:
            self._default_size = DEFAULT_SIZE
            body[SIZE_PARAM] = self._default_size

        curl = self.build_curl(db_url, 'POST', [('Content-Type', 'application/json')], body)

        _, stdout, stderr = self.ssh.exec_command(curl)
        out, err = stdout.read(), stderr.read()

        return out.decode().strip(), err.decode().strip()

    def get_scroll(self, scroll_id):

        db_url = furl(f'{self.db.host}:{self.db.port}')
        db_url /= '_search/scroll'

        data = {'scroll': self._default_scroll, 'scroll_id': scroll_id}

        curl = self.build_curl(db_url, 'POST', [('Content-Type', 'application/json')], data)

        _, stdout, stderr = self.ssh.exec_command(curl)
        out, err = stdout.read(), stderr.read()

        return out.decode().strip(), err.decode().strip()
