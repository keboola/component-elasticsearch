import json
import logging
import os
import shutil
import dateparser
import pytz
from typing import Union

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.csvwriter import ElasticDictWriter

from client.es_client import ElasticsearchClient
from client.ssh_client import SSHClient

# configuration variables
KEY_GROUP_DB = 'db'
KEY_DB_HOSTNAME = 'hostname'
KEY_DB_PORT = 'port'
KEY_QUERY = 'query'
KEY_INDEX_NAME = 'index_name'
KEY_STORAGE_TABLE = 'storage_table'
KEY_PRIMARY_KEYS = 'primary_keys'
KEY_INCREMENTAL = 'incremental'
KEY_GROUP_AUTH = 'authentication'
KEY_AUTH_TYPE = 'auth_type'
KEY_USERNAME = 'username'
KEY_PASSWORD = '#password'
KEY_API_KEY = '#api_key'
KEY_BEARER = '#bearer'
KEY_SCHEME = 'scheme'

KEY_GROUP_DATE = 'date'
KEY_DATE_APPEND = 'append_date'
KEY_DATE_FORMAT = 'format'
KEY_DATE_SHIFT = 'shift'
KEY_DATE_TZ = 'time_zone'
DATE_PLACEHOLDER = '{{date}}'
DEFAULT_DATE = 'yesterday'
DEFAULT_DATE_FORMAT = '%Y-%m-%d'
DEFAULT_TZ = 'UTC'

KEY_GROUP_SSH = 'ssh'
KEY_SSH_HOSTNAME = 'hostname'
KEY_SSH_PORT = 'port'
KEY_SSH_USERNAME = 'username'
KEY_SSH_PRIVATE_KEY = '#private_key'

REQUIRED_PARAMETERS = [KEY_GROUP_DB]

DEFAULT_QUERY = """
        query = {
            "query": {
                "match_all": {}
            }
        }
        """


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self.ssh_client = None

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        out_table_name = params.get(KEY_STORAGE_TABLE, "ex-elasticsearch-result")
        user_defined_pk = params.get(KEY_PRIMARY_KEYS, [])
        incremental = params.get(KEY_INCREMENTAL, False)

        index_name, query = self.parse_index_parameters(params)
        statefile_mapping = self.get_state_file()

        self.ssh_client = self.initialize_ssh_client(params)

        client = self.get_client(params)

        temp_folder = os.path.join(self.data_folder_path, "temp")
        os.makedirs(temp_folder, exist_ok=True)

        result_mapping = client.extract_data(index_name, query, temp_folder, out_table_name, statefile_mapping)

        mapping = self.extract_table_details(result_mapping)

        self.process_extracted_data(temp_folder, mapping, out_table_name, user_defined_pk, incremental)

        self.cleanup(temp_folder)
        self.write_state_file(result_mapping)

    @staticmethod
    def initialize_ssh_client(params) -> Union[SSHClient, None]:
        ssh_params = params.get(KEY_GROUP_SSH)

        if ssh_params.get(KEY_SSH_HOSTNAME, False):
            logging.info("Initializing SSH connection")
            ssh_host = ssh_params.get(KEY_SSH_HOSTNAME)
            ssh_port = ssh_params.get(KEY_SSH_PORT)
            ssh_username = ssh_params.get(KEY_SSH_USERNAME)
            ssh_private_key = ssh_params.get(KEY_SSH_PRIVATE_KEY)

            db_params = params.get(KEY_GROUP_DB)
            db_hostname = db_params.get(KEY_DB_HOSTNAME)
            db_port = db_params.get(KEY_DB_PORT)

            ssh_client = SSHClient(ssh_host, ssh_port, ssh_username, ssh_private_key)
            ssh_client.connect()
            ssh_client.setup_tunnel(db_hostname, db_port, db_port)
        else:
            ssh_client = None
        return ssh_client

    def process_extracted_data(self, temp_folder, mapping, out_table_name, user_defined_pk, incremental):
        for subfolder in os.listdir(temp_folder):
            logging.info(f"Processing data for table {subfolder}.")
            columns = mapping.get(subfolder, {}).get("columns", [])
            subfolder_path = os.path.join(temp_folder, subfolder)

            if subfolder != out_table_name:
                pk = mapping.get(subfolder, {}).get("primary_keys", [])
            else:
                pk = user_defined_pk

            out_table = self.create_out_table_definition(name=subfolder, primary_key=pk, incremental=incremental)
            logging.info(f"Processing table: {subfolder}, with primary keys: {pk}")
            with ElasticDictWriter(out_table.full_path, columns) as wr:
                wr.writeheader()
                for file in os.listdir(subfolder_path):
                    path = os.path.join(subfolder_path, file)
                    with open(path, 'r') as f:
                        rows = json.load(f)
                    wr.writerows(rows)

            self.write_manifest(out_table)

    def cleanup(self, temp_folder):
        shutil.rmtree(temp_folder)

        if self.ssh_client:
            self.ssh_client.close()

    def get_client(self, params: dict) -> ElasticsearchClient:
        auth_params = params.get(KEY_GROUP_AUTH)
        db_params = params.get(KEY_GROUP_DB)
        auth_type = auth_params.get(KEY_AUTH_TYPE, False)
        if not auth_type:
            return self.get_client_legacy(params)

        db_hostname = db_params.get(KEY_DB_HOSTNAME)
        db_port = db_params.get(KEY_DB_PORT)
        scheme = params.get(KEY_SCHEME, "http")

        if auth_type not in ["basic", "api_key", "bearer", "no_auth"]:
            raise UserException(f"Invalid auth_type: {auth_type}")

        setup = {"host": db_hostname, "port": db_port, "scheme": scheme}

        auth = None

        logging.info(f"The component will use {auth_type} type authorization.")
        if auth_type == "basic":
            username = auth_params.get(KEY_USERNAME)
            password = auth_params.get(KEY_PASSWORD)

            if not (username and password):
                raise UserException("You must specify both username and password for basic type authorization")

            auth = (username, password)

        elif auth_type in ["api_key", "bearer"]:
            token_key = {"api_key": KEY_API_KEY, "bearer": KEY_BEARER}[auth_type]
            token_value = auth_params.get(token_key)

            headers = {"Authorization": f"{auth_type.capitalize()} {token_value}"}
            setup["headers"] = headers

        client = ElasticsearchClient([setup], http_auth=auth)

        if not client.ping():
            raise UserException(f"Connection to Elasticsearch instance {db_hostname}:{db_port} failed")

        return client

    @staticmethod
    def get_client_legacy(params) -> ElasticsearchClient:
        db_params = params.get(KEY_GROUP_DB)
        db_hostname = db_params.get(KEY_DB_HOSTNAME)
        db_port = db_params.get(KEY_DB_PORT)

        setup = {"host": db_hostname, "port": db_port, "scheme": "http"}
        client = ElasticsearchClient([setup])

        return client

    def parse_index_parameters(self, params):
        index = params.get(KEY_INDEX_NAME, "")
        date_config = params.get(KEY_GROUP_DATE, {})
        query = self._parse_query(params)

        if DATE_PLACEHOLDER in index:
            index = self._replace_date_placeholder(index, date_config)

        return index, query

    @staticmethod
    def _parse_query(params):
        _query = params.get(KEY_QUERY, '{}').strip()
        query_string = _query if _query != '' else '{}'

        try:
            return json.loads(query_string)
        except ValueError:
            raise UserException("Could not parse request body string to JSON.")

    def _replace_date_placeholder(self, index, date_config):
        _date = dateparser.parse(date_config.get(KEY_DATE_SHIFT, DEFAULT_DATE))
        if _date is None:
            raise UserException(f"Could not parse value {date_config[KEY_DATE_SHIFT]} to date.")

        _date = _date.replace(tzinfo=pytz.UTC)
        _tz = self._validate_timezone(date_config.get(KEY_DATE_TZ, DEFAULT_TZ))
        _date_tz = pytz.timezone(_tz).normalize(_date)
        _date_formatted = _date_tz.strftime(date_config.get(KEY_DATE_FORMAT, DEFAULT_DATE_FORMAT))

        logging.info(f"Replaced date placeholder with value {_date_formatted}. "
                     f"Downloading data from index {index.replace(DATE_PLACEHOLDER, _date_formatted)}.")
        return index.replace(DATE_PLACEHOLDER, _date_formatted)

    @staticmethod
    def _validate_timezone(tz):
        if tz not in pytz.all_timezones:
            raise UserException(f"Incorrect timezone {tz} provided. Timezone must be a valid DB timezone name. "
                                "See https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List.")
        return tz

    def extract_table_details(self, data, parent_prefix=''):
        output = {}

        # Get the current table name with any necessary prefixes
        current_table_name = parent_prefix + data["table_name"]

        # Store columns and primary keys for the current table
        output[current_table_name] = {
            "columns": list(data["column_mappings"].values()),
            "primary_keys": data["primary_keys"]
        }

        # If there are child tables, extract details recursively for each child table
        for child_name, child_data in data["child_tables"].items():
            output.update(self.extract_table_details(child_data, current_table_name + "_"))

        return output


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
