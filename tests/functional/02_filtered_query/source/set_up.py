from keboola.datadirtest import TestDataDir
from elasticsearch import Elasticsearch


def run(context: TestDataDir):
    es = Elasticsearch("http://elasticsearch:9200")

    if es.indices.exists(index="test-users"):
        es.indices.delete(index="test-users")

    documents = [
        {"user_id": 1, "name": "Adam", "age": 30, "active": True},
        {"user_id": 2, "name": "Božena", "age": 31, "active": True},
        {"user_id": 3, "name": "Cecil", "age": 32, "active": False},
    ]

    for doc in documents:
        es.index(index="test-users", id=doc["user_id"], document=doc)

    es.indices.refresh(index="test-users")
