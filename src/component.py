import json
import logging
import os
import shutil
import dateparser
import pytz

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.csvwriter import ElasticDictWriter

from client.es_client import ElasticsearchClient

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

KEY_DATE = 'date'
KEY_DATE_APPEND = 'append_date'
KEY_DATE_FORMAT = 'format'
KEY_DATE_SHIFT = 'shift'
KEY_DATE_TZ = 'time_zone'
DATE_PLACEHOLDER = '{{date}}'
DEFAULT_DATE = 'yesterday'
DEFAULT_DATE_FORMAT = '%Y-%m-%d'
DEFAULT_TZ = 'UTC'


REQUIRED_PARAMETERS = [KEY_GROUP_DB]

DEFAULT_QUERY = """
        query = {
            "query": {
                "match_all": {}
            }
        }
        """


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters
        auth = params.get(KEY_GROUP_AUTH)
        out_table_name = params.get(KEY_STORAGE_TABLE, "ex-elasticsearch-result")
        pks = params.get(KEY_PRIMARY_KEYS, [])
        incremental = params.get(KEY_INCREMENTAL, False)

        index_name, query = self.parse_index_parameters(params)

        client = self.get_client(params)

        statefile = self.get_state_file()
        previous_mapping = statefile.get(out_table_name, None)

        out_table = self.create_out_table_definition(name=out_table_name, primary_key=pks, incremental=incremental)

        temp_folder = os.path.join(self.data_folder_path, "temp")
        os.makedirs(temp_folder, exist_ok=True)

        parser = client.extract_data(index_name, query, temp_folder, out_table_name, previous_mapping)
        table_mappings = parser.get_table_mapping()
        columns = list(table_mappings['column_mappings'].values())

        with ElasticDictWriter(out_table.full_path, columns) as wr:
            wr.writeheader()
            for file in os.listdir(temp_folder):
                path = os.path.join(temp_folder, file)
                with open(path, 'r') as f:
                    data = json.load(f)
                rows = data.get(out_table_name)
                wr.writerows(rows)

        self.write_manifest(out_table)

        self.write_state_file({out_table_name: table_mappings})

        shutil.rmtree(temp_folder)

    @staticmethod
    def get_client(params: dict) -> ElasticsearchClient:
        auth_params = params.get(KEY_GROUP_AUTH)
        db_params = params.get(KEY_GROUP_DB)
        auth_type = auth_params.get(KEY_AUTH_TYPE)

        db_hostname = db_params.get(KEY_DB_HOSTNAME)
        db_port = db_params.get(KEY_DB_PORT)

        setup = {"host": db_hostname, "port": db_port, "scheme": "http"}

        if auth_type == "basic":
            username = auth_params.get(KEY_USERNAME)
            password = auth_params.get(KEY_PASSWORD)
            setup["http_auth"] = (username, password)
        elif auth_type == "api_key":
            api_key = auth_params.get(KEY_API_KEY)
            headers = {"Authorization": f"ApiKey {api_key}"}
            setup["headers"] = headers
        elif auth_type == "bearer":
            bearer = auth_params.get(KEY_BEARER)
            headers = {"Authorization": f"Bearer {bearer}"}
            setup["headers"] = headers
        elif auth_type == "no_auth":
            pass
        else:
            raise UserException(f"Invalid auth_type: {auth_type}")

        es_params = [setup]

        client = ElasticsearchClient(es_params)

        # Check if the connection is established
        if not client.ping():
            raise UserException(f"Connection to Elasticsearch instance {db_hostname}:{db_port} failed!")

        return client

    def parse_index_parameters(self, params):
        index = params[KEY_INDEX_NAME]
        date_config = params.get(KEY_DATE, {})
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
