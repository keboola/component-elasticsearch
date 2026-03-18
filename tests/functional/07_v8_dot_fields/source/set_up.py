from keboola.datadirtest import TestDataDir
from elasticsearch import Elasticsearch


def run(context: TestDataDir):
    es = Elasticsearch("http://elasticsearch8:9200")

    if es.indices.exists(index="test-dot-fields"):
        es.indices.delete(index="test-dot-fields")

    # Documents with dot-notation field names (e.g. from nested objects flattened by ES)
    documents = [
        {"id": 1, "response.status": 200, "response.time_ms": 120},
        {"id": 2, "response.status": 404, "response.time_ms": 45},
    ]

    for doc in documents:
        es.index(index="test-dot-fields", id=doc["id"], document=doc)

    es.indices.refresh(index="test-dot-fields")
