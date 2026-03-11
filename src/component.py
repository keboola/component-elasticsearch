import json
import logging
import shutil
import os
import uuid

import dateparser
import paramiko
import pytz

# sshtunnel 0.4.0 references paramiko.DSSKey which was removed in paramiko 3.x
if not hasattr(paramiko, "DSSKey"):
    paramiko.DSSKey = type("DSSKey", (), {})

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.csvwriter import ElasticDictWriter

from client.es_client import ElasticsearchClient
from client.ssh_utils import SomeSSHException, get_private_key
from configuration import AuthType, Configuration
from legacy_client.legacy_es_client import LegacyClient
from sshtunnel import BaseSSHTunnelForwarderError, SSHTunnelForwarder

LOCAL_BIND_ADDRESS = "127.0.0.1"

RSA_HEADER = "-----BEGIN RSA PRIVATE KEY-----"

DATE_PLACEHOLDER = "{{date}}"


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(["db"])
        params = self.configuration.parameters

        config = Configuration(**params)

        if config.ssh is not None:
            self.run_legacy_client()
            return

        out_table_name = config.storage_table
        logging.info(f"Using output table name: {out_table_name}")

        index_name, query = self.parse_index_parameters(config)
        statefile = self.get_state_file()

        use_ssh_tunnel = False
        ssh_opts = config.ssh_options
        # Guard against KBC returning ssh_options as an empty list instead of null
        if ssh_opts is not None and ssh_opts.enabled:
            self._create_and_start_ssh_tunnel(config)
            use_ssh_tunnel = True

        hostname_override = LOCAL_BIND_ADDRESS if use_ssh_tunnel else None
        client = self.get_client(config, hostname_override=hostname_override)

        temp_folder = os.path.join(self.data_folder_path, "temp")
        os.makedirs(temp_folder, exist_ok=True)

        columns = statefile.get(out_table_name, [])
        out_table = self.create_out_table_definition(
            out_table_name,
            primary_key=config.primary_keys,
            incremental=config.incremental,
        )

        try:
            with ElasticDictWriter(out_table.full_path, columns) as wr:
                for result in client.extract_data(index_name, query, include_meta_fields=config.include_meta_fields):
                    wr.writerow(result)
                wr.writeheader()
        except Exception as e:
            raise UserException(f"Error occured while extracting data from Elasticsearch: {e}")
        finally:
            if hasattr(self, "ssh_server") and self.ssh_server.is_active:
                self.ssh_server.stop()

        self.write_manifest(out_table)
        statefile[out_table_name] = wr.fieldnames
        self.write_state_file(statefile)
        self.cleanup(temp_folder)

    @staticmethod
    def run_legacy_client() -> None:
        client = LegacyClient()
        client.run()

    @staticmethod
    def cleanup(temp_folder: str):
        shutil.rmtree(temp_folder)

    def get_client(self, config: Configuration, hostname_override: str = None) -> ElasticsearchClient:
        auth = config.authentication
        if auth is None:
            return self.get_client_legacy(config)

        db = config.db
        db_hostname = hostname_override if hostname_override else db.hostname
        db_port = db.port
        scheme = config.scheme

        setup = {"host": db_hostname, "port": db_port, "scheme": scheme}

        logging.info(f"The component will use {auth.auth_type} type authorization.")

        if auth.auth_type == AuthType.basic:
            http_auth = (auth.username, auth.password)
            client = ElasticsearchClient([setup], scheme, http_auth=http_auth)

        elif auth.auth_type == AuthType.api_key:
            api_key_tuple = (auth.api_key_id, auth.api_key)
            client = ElasticsearchClient([setup], scheme, api_key=api_key_tuple)

        elif auth.auth_type == AuthType.no_auth:
            client = ElasticsearchClient([setup], scheme)

        else:
            raise UserException(f"Unsupported auth_type: {auth.auth_type}")

        try:
            p = client.ping(error_trace=True)
            if not p:
                raise UserException(f"Connection to Elasticsearch instance {db_hostname}:{db_port} failed.")
        except Exception as e:
            raise UserException(f"Connection to Elasticsearch instance {db_hostname}:{db_port} failed. {str(e)}")

        return client

    @staticmethod
    def get_client_legacy(config: Configuration) -> ElasticsearchClient:
        db = config.db
        setup = {"host": db.hostname, "port": db.port, "scheme": "http"}
        return ElasticsearchClient([setup])

    def parse_index_parameters(self, config: Configuration):
        index = config.index_name
        query = self._parse_query(config.request_body)

        if DATE_PLACEHOLDER in index:
            index = self._replace_date_placeholder(index, config)

        return index, query

    @staticmethod
    def _parse_query(request_body: str) -> dict:
        query_string = request_body.strip() or "{}"

        try:
            logging.info(f"Using query: {query_string}")
            return json.loads(query_string)
        except ValueError:
            raise UserException("Could not parse request body string to JSON.")

    def _replace_date_placeholder(self, index: str, config: Configuration) -> str:
        date_cfg = config.date
        _date = dateparser.parse(date_cfg.shift)
        if _date is None:
            raise UserException(f"Could not parse value {date_cfg.shift} to date.")

        _date = _date.replace(tzinfo=pytz.UTC)
        _tz = self._validate_timezone(date_cfg.time_zone)
        _date_tz = pytz.timezone(_tz).normalize(_date)
        _date_formatted = _date_tz.strftime(date_cfg.format)

        logging.info(
            f"Replaced date placeholder with value {_date_formatted}. "
            f"Downloading data from index {index.replace(DATE_PLACEHOLDER, _date_formatted)}."
        )
        return index.replace(DATE_PLACEHOLDER, _date_formatted)

    @staticmethod
    def _validate_timezone(tz: str) -> str:
        if tz not in pytz.all_timezones:
            raise UserException(
                f"Incorrect timezone {tz} provided. Timezone must be a valid DB timezone name. "
                "See https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List."
            )
        return tz

    @staticmethod
    def _save_results(results: list, destination: str) -> None:
        full_path = os.path.join(destination, f"{uuid.uuid4()}.json")
        with open(full_path, "w") as json_file:
            json.dump(results, json_file, indent=4)

    def _create_and_start_ssh_tunnel(self, config: Configuration) -> None:
        ssh = config.ssh_options
        db = config.db
        private_key = ssh.keys.private_key if ssh.keys else None
        self._create_ssh_tunnel(
            ssh_username=ssh.user,
            private_key=private_key,
            ssh_tunnel_host=ssh.sshHost,
            ssh_tunnel_port=ssh.sshPort,
            db_hostname=db.hostname,
            db_port=db.port,
        )

        try:
            self.ssh_server.start()
        except BaseSSHTunnelForwarderError as e:
            raise UserException("Failed to establish SSH connection. Recheck all SSH configuration parameters") from e

        logging.info("SSH tunnel is enabled.")

    @staticmethod
    def is_valid_rsa(rsa_key: str) -> tuple[bool, str]:
        if not rsa_key.startswith(RSA_HEADER):
            return (
                False,
                f"The RSA key does not start with the correct header: {RSA_HEADER}",
            )
        if "\n" not in rsa_key:
            return False, "The RSA key does not contain any newline characters."
        return True, ""

    def _create_ssh_tunnel(
        self,
        ssh_username: str,
        private_key: str,
        ssh_tunnel_host: str,
        ssh_tunnel_port: int,
        db_hostname: str,
        db_port: int,
    ) -> None:
        is_valid, error_message = self.is_valid_rsa(private_key)
        if is_valid:
            logging.info("SSH tunnel is enabled.")
        else:
            raise UserException(f"Invalid RSA key provided: {error_message}")

        try:
            private_key = get_private_key(private_key, None)
        except SomeSSHException as e:
            raise UserException(e) from e

        self.ssh_server = SSHTunnelForwarder(
            ssh_address_or_host=ssh_tunnel_host,
            ssh_port=ssh_tunnel_port,
            ssh_pkey=private_key,
            ssh_username=ssh_username,
            remote_bind_address=(db_hostname, db_port),
            local_bind_address=(LOCAL_BIND_ADDRESS, db_port),
            ssh_config_file=None,
            allow_agent=False,
        )


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
