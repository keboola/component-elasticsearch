import json
import logging
import typing as t
from typing import Iterable

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError, TransportError

DEFAULT_SIZE = 10_000
SCROLL_TIMEOUT = "15m"
DEFAULT_PIT_KEEP_ALIVE = "5m"


class ElasticsearchClientException(Exception):
    pass


class ElasticsearchClient(Elasticsearch):
    def __init__(self, hosts: list, scheme: str = None, http_auth: tuple = None, api_key: tuple = None):
        options = {"hosts": hosts, "request_timeout": 30, "retry_on_timeout": True, "max_retries": 5}

        if scheme == "https":
            options.update({"verify_certs": False, "ssl_show_warn": False})

        if http_auth:
            options.update({"basic_auth": http_auth})
        elif api_key:
            options.update({"api_key": api_key})

        super().__init__(**options)

    META_FIELDS = ("_id", "_index", "_type", "_score", "_ignored")

    def extract_data(self, index_name: str, query: str, include_meta_fields: bool = False) -> Iterable:
        """
        Extracts data using the Scroll API.

        Parameters:
            index_name (str): Name of the Elasticsearch index.
            query (dict): Elasticsearch DSL query.
            include_meta_fields (bool): When True, merges ES metadata fields (_id, _index, etc.) into each row.

        Yields:
            dict
        """
        response = self.search(index=index_name, size=DEFAULT_SIZE, scroll=SCROLL_TIMEOUT, body=query)
        for r in self._process_response(response, include_meta_fields):
            yield r

        while len(response["hits"]["hits"]):
            response = self.scroll(scroll_id=response["_scroll_id"], scroll=SCROLL_TIMEOUT)
            for r in self._process_response(response, include_meta_fields):
                yield r

    def extract_data_pit(
        self, index_name: str, query: dict, include_meta_fields: bool = False, keep_alive: str = DEFAULT_PIT_KEEP_ALIVE
    ) -> Iterable:
        """
        Extracts data using PIT (Point-in-Time) + search_after pagination.

        Parameters:
            index_name (str): Name of the Elasticsearch index.
            query (dict): Elasticsearch DSL query. A "size" set in the query is used as the page size,
                DEFAULT_SIZE is applied only when the query does not specify it.
            include_meta_fields (bool): When True, merges ES metadata fields into each row.
            keep_alive (str): How long the PIT should be kept alive between requests.

        Yields:
            dict
        """
        pit = self.open_point_in_time(index=index_name, keep_alive=keep_alive)
        pit_id = pit["id"]

        try:
            search_body = {**query, "pit": {"id": pit_id, "keep_alive": keep_alive}}
            search_body.setdefault("size", DEFAULT_SIZE)

            if "sort" not in search_body:
                search_body["sort"] = [{"_shard_doc": "asc"}]

            response = self.search(body=search_body)
            pit_id = response.get("pit_id", pit_id)
            for r in self._process_response(response, include_meta_fields):
                yield r

            while len(response["hits"]["hits"]):
                last_hit = response["hits"]["hits"][-1]
                search_body["search_after"] = last_hit["sort"]
                pit_id = response.get("pit_id", pit_id)
                search_body["pit"] = {"id": pit_id, "keep_alive": keep_alive}

                response = self.search(body=search_body)
                for r in self._process_response(response, include_meta_fields):
                    yield r
        finally:
            try:
                self.close_point_in_time(id=pit_id)
            except (ApiError, TransportError) as e:
                logging.warning(
                    f"Failed to close the Point-in-Time {pit_id}: {e}. "
                    f"It will stay open on the Elasticsearch cluster until its keep_alive ({keep_alive}) expires."
                )

    def _process_response(self, response: dict, include_meta_fields: bool = False) -> Iterable:
        for hit in response["hits"]["hits"]:
            row = self.flatten_json(hit["_source"])
            if include_meta_fields:
                meta = {field: hit.get(field) for field in self.META_FIELDS if field in hit}
                row = {**meta, **row}
            yield row

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

    def flatten_json(self, x, out=None, name=""):
        if out is None:
            out = dict()
        if type(x) is dict:
            for a in x:
                self.flatten_json(x[a], out, name + a + ".")

        elif type(x) is list:
            out[name[:-1]] = json.dumps(x)

        else:
            out[name[:-1]] = x

        return out
