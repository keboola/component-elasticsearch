from elasticsearch import Elasticsearch
import uuid
import json
import os

from keboola.json_to_csv import Parser


DEFAULT_SIZE = 1000
SCROLL_TIMEOUT = '15m'


class ElasticsearchClientException(Exception):
    pass


class ElasticsearchClient(Elasticsearch):

    def __init__(self, params: list[dict]):
        super().__init__(params)
        self.parser = None

    def extract_data(self, index_name: str, query: str, destination: str, table_name: str, parser: Parser):
        """
        Extracts data from the specified Elasticsearch index based on the given query.

        Parameters:
            index_name (str): Name of the Elasticsearch index.
            query (dict): Elasticsearch DSL query.
            destination (str): Path to store the results to.
            table_name (str): Name of the output table - only used for parser.
            parser (Parser): Uses already existing Parser setup.

        Returns:
            TableMapping
        """
        if not self.parser:
            self.parser = Parser(main_table_name=table_name, analyze_further=True)
        else:
            self.parser = parser

        response = self.search(index=index_name, size=DEFAULT_SIZE, scroll=SCROLL_TIMEOUT, body=query)
        self._save_results([hit["_source"] for hit in response['hits']['hits']], destination)

        while len(response['hits']['hits']):
            response = self.scroll(scroll_id=response["_scroll_id"], scroll=SCROLL_TIMEOUT)
            self._save_results([hit["_source"] for hit in response['hits']['hits']], destination)

        return self.parser

    def _save_results(self, results: list, destination: str) -> None:
        parsed = self.parser.parse_data(results)

        for table in parsed:
            table_folder_path = os.path.join(destination, table)
            os.makedirs(table_folder_path, exist_ok=True)

            filename = f"{uuid.uuid4()}.json"
            filepath = os.path.join(table_folder_path, filename)

            with open(filepath, 'w') as file:
                json.dump(parsed[table], file, indent=4)
