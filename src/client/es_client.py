import json
import typing as t
from typing import Iterable

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ApiError, TransportError

DEFAULT_SIZE = 10_000
SCROLL_TIMEOUT = '15m'

# Exponential backoff configuration for ES requests
ES_INITIAL_DELAY = 2.0
ES_MAX_DELAY = 30.0
ES_BACKOFF_MULTIPLIER = 2.0
ES_MAX_RETRIES = 10


class ElasticsearchClientException(Exception):
    pass


class ElasticsearchClient(Elasticsearch):

    def __init__(self, hosts: list, scheme: str = None, http_auth: tuple = None, api_key: tuple = None):
        # Configure retry behavior with exponential backoff
        # The Elasticsearch client handles exponential backoff internally when max_retries > 0
        options = {
            "hosts": hosts,
            "timeout": 30,                    # Request timeout in seconds
            "retry_on_timeout": True,         # Retry on request timeouts
            "max_retries": 10,                 # Maximum number of retries (enables exponential backoff)
            # "retry_on_status": [429, 500, 502, 503, 504],  # HTTP status codes to retry on
            "sniff_on_start": True,          # Don't sniff on startup to avoid connection issues
            "sniff_on_connection_fail": True,  # Sniff when connection fails to find healthy nodes
        }

        if scheme == "https":
            options.update({"verify_certs": False, "ssl_show_warn": False})

        if http_auth:
            options.update({"http_auth": http_auth})
        elif api_key:
            options.update({"api_key": api_key})

        super().__init__(**options)

    def extract_data(self, index_name: str, query: str) -> Iterable:
        """
        Extracts data from the specified Elasticsearch index based on the given query.

        Parameters:
            index_name (str): Name of the Elasticsearch index.
            query (dict): Elasticsearch DSL query.

        Yields:
            dict
        """
        response = self._with_backoff(
            lambda: self.search(index=index_name, size=DEFAULT_SIZE, scroll=SCROLL_TIMEOUT, body=query),
            op_name="search"
        )
        for r in self._process_response(response):
            yield r

        while len(response['hits']['hits']):
            response = self._with_backoff(
                lambda: self.scroll(scroll_id=response["_scroll_id"], scroll=SCROLL_TIMEOUT),
                op_name="scroll"
            )
            for r in self._process_response(response):
                yield r

    def _process_response(self, response: dict) -> Iterable:
        results = [hit["_source"] for hit in response['hits']['hits']]
        for result in results:
            yield self.flatten_json(result)

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

    def _with_backoff(self, func, op_name: str):
        """
        Execute an ES operation with exponential backoff and jitter.
        Retries on TransportError and ApiError with retryable status codes.
        """
        attempt = 0
        last_error = None
        while attempt < ES_MAX_RETRIES:
            try:
                return func()
            except TransportError as e:
                last_error = e
                # Determine if status is retryable if present
                status = getattr(e, 'status_code', None)
                if status is not None and status not in [429, 500, 502, 503, 504]:
                    raise
            except ApiError as e:
                last_error = e
                status = getattr(e, 'status_code', None)
                if status is not None and status not in [429, 500, 502, 503, 504]:
                    raise

            # Compute delay
            delay = min(ES_INITIAL_DELAY * (ES_BACKOFF_MULTIPLIER ** attempt), ES_MAX_DELAY)
            attempt += 1
            # Log and sleep
            # Two spaces before inline comment are intentional per linter
            # Add small jitter via Python's random without importing globally
            from random import random as _rand
            jitter = delay * 0.1 * (2 * _rand() - 1)
            final_delay = max(0.1, delay + jitter)
            # Keep logs concise; external system shows attempt indexes starting at 1
            print(f"Retrying {op_name} after failure (attempt {attempt} of {ES_MAX_RETRIES}), sleeping {final_delay:.2f}s")
            import time as _time
            _time.sleep(final_delay)

        # Exhausted
        if last_error is not None:
            raise last_error
        raise ElasticsearchClientException(f"{op_name} failed after {ES_MAX_RETRIES} retries")

    def flatten_json(self, x, out=None, name=''):
        if out is None:
            out = dict()
        if type(x) is dict:
            for a in x:
                self.flatten_json(x[a], out, name + a + '.')

        elif type(x) is list:
            out[name[:-1]] = json.dumps(x)

        else:
            out[name[:-1]] = x

        return out
