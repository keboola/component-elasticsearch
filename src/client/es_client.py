from elasticsearch import Elasticsearch
import uuid
import json
import os

from keboola.json_to_csv import Parser, TableMapping

DEFAULT_SIZE = 1000
SCROLL_TIMEOUT = '15m'


class ElasticsearchClientException(Exception):
    pass


class ElasticsearchClient(Elasticsearch):

    def __init__(self, hosts: list, http_auth: tuple = None):
        super().__init__(hosts, http_auth=http_auth)

    def extract_data(self, index_name: str, query: str, destination: str, table_name: str,
                     mapping: dict = None) -> dict:
        """
        Extracts data from the specified Elasticsearch index based on the given query.

        Parameters:
            index_name (str): Name of the Elasticsearch index.
            query (dict): Elasticsearch DSL query.
            destination (str): Path to store the results to.
            table_name (str): Name of the output table - only used for parser.
            mapping (dict): Uses already existing Parser setup.

        Returns:
            dict
        """
        parser = self._initialize_parser(table_name, mapping)

        response = self.search(index=index_name, size=DEFAULT_SIZE, scroll=SCROLL_TIMEOUT, body=query)
        self._process_response(response, parser, destination)

        while len(response['hits']['hits']):
            response = self.scroll(scroll_id=response["_scroll_id"], scroll=SCROLL_TIMEOUT)
            self._process_response(response, parser, destination)

        return parser.table_mapping.as_dict()

    @staticmethod
    def _initialize_parser(table_name: str, mapping: dict = None) -> Parser:
        if not mapping:
            return Parser(main_table_name=table_name, analyze_further=True)
        else:
            table_mapping = TableMapping.build_from_mapping_dict(mapping)
            return Parser(table_name, table_mapping=table_mapping)

    def _process_response(self, response: dict, parser: Parser, destination: str) -> None:
        results = [hit["_source"] for hit in response['hits']['hits']]
        parsed = parser.parse_data(results)
        self._save_results(parsed, destination)
        parser._csv_file_results = {}

    @staticmethod
    def _save_results(results: dict, destination: str) -> None:
        for result in results:
            path = os.path.join(destination, result)
            os.makedirs(path, exist_ok=True)

            full_path = os.path.join(path, f"{uuid.uuid4()}.json")
            with open(full_path, "w") as json_file:
                json.dump(results.get(result), json_file, indent=4)
