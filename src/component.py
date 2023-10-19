import json
import logging
import os
import shutil

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.csvwriter import ElasticDictWriter

from client.es_client import ElasticsearchClient


# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_PRINT_HELLO = 'print_hello'
KEY_GROUP_DB = 'db'
KEY_DB_HOSTNAME = 'hostname'
KEY_DB_PORT = 'port'
KEY_QUERY = 'query'
KEY_INDEX_NAME = 'index_name'
KEY_STORAGE_TABLE = 'storage_table'

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
        out_table_name = params.get(KEY_STORAGE_TABLE, "ex-elasticsearch-result")

        query = params.get(KEY_QUERY, DEFAULT_QUERY)
        index_name = params.get(KEY_INDEX_NAME)

        client = self.get_client(params)

        statefile = self.get_state_file()
        previous_mapping = statefile.get(out_table_name, None)

        out_table = self.create_out_table_definition(out_table_name)

        temp_folder = os.path.join(self.data_folder_path, "temp")
        os.makedirs(temp_folder, exist_ok=True)

        parser = client.extract_data(index_name, query, temp_folder, out_table_name, previous_mapping)
        table_mappings = parser.get_table_mapping()

        with ElasticDictWriter(out_table.full_path, []) as wr:
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
        db_params = params.get(KEY_GROUP_DB)

        db_hostname = db_params.get(KEY_DB_HOSTNAME)
        db_port = db_params.get(KEY_DB_PORT)

        client = ElasticsearchClient(db_hostname, db_port)

        # Check if the connection is established
        if not client.ping():
            raise UserException(f"Connection to Elasticsearch instance {db_hostname}:{db_port} failed!")

        return client


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
