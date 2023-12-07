import json
import logging
import uuid
import os
import shutil
import dateparser
import pytz

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.csvwriter import ElasticDictWriter
from keboola.json_to_csv import Parser, TableMapping

from client.es_client import ElasticsearchClient
from legacy_client.legacy_es_client import LegacyClient


# configuration variables
KEY_GROUP_DB = 'db'
KEY_DB_HOSTNAME = 'hostname'
KEY_DB_PORT = 'port'
KEY_QUERY = 'request_body'  # this is named like this for backwards compatibility
KEY_INDEX_NAME = 'index_name'
KEY_STORAGE_TABLE = 'storage_table'
KEY_PRIMARY_KEYS = 'primary_keys'
KEY_INCREMENTAL = 'incremental'
KEY_GROUP_AUTH = 'authentication'
KEY_AUTH_TYPE = 'auth_type'
KEY_USERNAME = 'username'
KEY_PASSWORD = '#password'
KEY_API_KEY_ID = 'api_key_id'
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

KEY_LEGACY_SSH = 'ssh'

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

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        params = self.configuration.parameters

        if params.get(KEY_LEGACY_SSH):
            self.run_legacy_client()
        else:
            out_table_name = params.get(KEY_STORAGE_TABLE, False)
            if not out_table_name:
                out_table_name = "ex-elasticsearch-result"
                logging.info(f"Using default output table name: {out_table_name}")

            user_defined_pk = params.get(KEY_PRIMARY_KEYS, [])
            incremental = params.get(KEY_INCREMENTAL, False)

            index_name, query = self.parse_index_parameters(params)
            statefile_mapping = self.get_state_file()
            client = self.get_client(params)

            self.temp_folder = os.path.join(self.data_folder_path, "temp")
            self.raw_folder = os.path.join(self.temp_folder, "raw")
            self.processed_folder = os.path.join(self.temp_folder, "processed")
            os.makedirs(self.raw_folder, exist_ok=True)
            os.makedirs(self.processed_folder, exist_ok=True)

            client.extract_data(index_name, query, self.raw_folder)

            parser = self._initialize_parser(out_table_name, statefile_mapping)

            result_mapping = self.process_extracted_data(parser, out_table_name, user_defined_pk, incremental)

            self.cleanup()
            self.write_state_file(result_mapping)

    @staticmethod
    def run_legacy_client() -> None:
        client = LegacyClient()
        client.run()

    @staticmethod
    def _save_results(results: dict, destination: str) -> None:
        for result in results:
            path = os.path.join(destination, result)
            os.makedirs(path, exist_ok=True)

            full_path = os.path.join(path, f"{uuid.uuid4()}.json")
            with open(full_path, "w") as json_file:
                json.dump(results.get(result), json_file, indent=4)

    def process_extracted_data(self, parser, out_table_name, user_defined_pk, incremental):
        logging.info("Parsing raw data.")
        for file in os.listdir(self.raw_folder):
            filepath = os.path.join(self.raw_folder, file)
            with open(filepath, 'r') as in_f:
                data = json.load(in_f)
                parsed = parser.parse_data(data)
                self._save_results(parsed, self.processed_folder)

            # this is a hack to prevent oom
            for table in parser._csv_file_results:
                parser._csv_file_results[table].rows = []

        result_mapping = parser.table_mapping.as_dict()
        mapping = self.extract_table_details(result_mapping)

        for subfolder in os.listdir(self.processed_folder):

            columns = mapping.get(subfolder, {}).get("columns", [])
            subfolder_path = os.path.join(self.processed_folder, subfolder)

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

        return result_mapping

    def cleanup(self):
        shutil.rmtree(self.temp_folder)

    def get_client(self, params: dict) -> ElasticsearchClient:
        auth_params = params.get(KEY_GROUP_AUTH)
        if not auth_params:
            return self.get_client_legacy(params)

        db_params = params.get(KEY_GROUP_DB)
        db_hostname = db_params.get(KEY_DB_HOSTNAME)
        db_port = db_params.get(KEY_DB_PORT)
        scheme = params.get(KEY_SCHEME, "http")

        auth_type = auth_params.get(KEY_AUTH_TYPE, False)
        if auth_type not in ["basic", "api_key", "bearer", "no_auth"]:
            raise UserException(f"Invalid auth_type: {auth_type}")

        setup = {"host": db_hostname, "port": db_port, "scheme": scheme}

        logging.info(f"The component will use {auth_type} type authorization.")

        if auth_type == "basic":
            username = auth_params.get(KEY_USERNAME)
            password = auth_params.get(KEY_PASSWORD)

            if not (username and password):
                raise UserException("You must specify both username and password for basic type authorization")

            auth = (username, password)
            client = ElasticsearchClient([setup], scheme, http_auth=auth)
        elif auth_type == "api_key":
            api_key_id = auth_params.get(KEY_API_KEY_ID)
            api_key = auth_params.get(KEY_API_KEY)
            api_key = (api_key_id, api_key)
            client = ElasticsearchClient([setup], scheme, api_key=api_key)
        elif auth_type == "no_auth":
            client = ElasticsearchClient([setup], scheme)
        else:
            raise UserException(f"Unsupported auth_type: {auth_type}")

        if not client.ping(error_trace=True):
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
            logging.info(f"Using query: {query_string}")
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

    def extract_table_details(self, data, parent_prefix='', output=None):
        if output is None:
            output = {}

        current_table_name = parent_prefix + data["table_name"]

        # Store columns and primary keys for the current table
        output[current_table_name] = {
            "columns": list(data["column_mappings"].values()),
            "primary_keys": data["primary_keys"]
        }

        # If there are child tables, extract details recursively for each child table
        for child_name, child_data in data["child_tables"].items():
            child_output = self.extract_table_details(child_data, current_table_name + "_")
            output.update(child_output)

        return output

    @staticmethod
    def _initialize_parser(table_name: str, mapping: dict = None) -> Parser:
        if not mapping:
            return Parser(main_table_name=table_name, analyze_further=True)
        else:
            table_mapping = TableMapping.build_from_mapping_dict(mapping)
            return Parser(table_name, table_mapping=table_mapping)


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
