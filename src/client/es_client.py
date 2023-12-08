import uuid
import json
import os
import typing as t

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError, TransportError

DEFAULT_SIZE = 10_000
SCROLL_TIMEOUT = '15m'


class ElasticsearchClientException(Exception):
    pass


class ElasticsearchClient(Elasticsearch):

    def __init__(self, hosts: list, scheme: str = None, http_auth: tuple = None, api_key: tuple = None):
        options = {"hosts": hosts}

        if scheme == "https":
            options.update({"verify_certs": False, "ssl_show_warn": False})

        if http_auth:
            options.update({"http_auth": http_auth})
        elif api_key:
            options.update({"api_key": api_key})

        super().__init__(**options)

    def extract_data(self, index_name: str, query: str, destination: str) -> None:
        """
        Extracts data from the specified Elasticsearch index based on the given query.

        Parameters:
            index_name (str): Name of the Elasticsearch index.
            query (dict): Elasticsearch DSL query.
            destination (str): Path to store the results to.

        Returns:
            dict
        """

        response = self.search(index=index_name, size=DEFAULT_SIZE, scroll=SCROLL_TIMEOUT, body=query)
        os.makedirs(destination, exist_ok=True)
        self._process_response(response, destination)

        while len(response['hits']['hits']):
            response = self.scroll(scroll_id=response["_scroll_id"], scroll=SCROLL_TIMEOUT)
            self._process_response(response, destination)

    def _process_response(self, response: dict, destination: str) -> None:
        results = [hit["_source"] for hit in response['hits']['hits']]
        self._save_results(results, destination)

    @staticmethod
    def _save_results(results: list, destination: str) -> None:
        full_path = os.path.join(destination, f"{uuid.uuid4()}.json")
        with open(full_path, "w") as json_file:
            json.dump(results, json_file, indent=4)

    def ping(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[t.List[str], str]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> bool:
        """
        Returns True if a successful response returns from the info() API,
        otherwise returns False. This API call can fail either at the transport
        layer (due to connection errors or timeouts) or from a non-2XX HTTP response
        (due to authentication or authorization issues).

        If you want to discover why the request failed you should use the ``info()`` API.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html>`_
        """
        __path = "/"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        try:
            self.perform_request("HEAD", __path, params=__query, headers=__headers)
            return True
        except (ApiError, TransportError) as e:
            raise ElasticsearchClientException(e)
