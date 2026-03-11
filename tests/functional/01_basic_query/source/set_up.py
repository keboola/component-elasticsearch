from keboola.datadirtest import TestDataDir
from elasticsearch import Elasticsearch


def run(context: TestDataDir):
    es = Elasticsearch("http://elasticsearch:9200")

    if es.indices.exists(index="test-products"):
        es.indices.delete(index="test-products")

    documents = [
        {"product_id": 1, "name": "Apples", "category": "fruit", "price": 1},
        {"product_id": 2, "name": "Bananas", "category": "fruit", "price": 2},
        {"product_id": 3, "name": "Citrons", "category": "fruit", "price": 3},
    ]

    for doc in documents:
        es.index(index="test-products", id=doc["product_id"], document=doc)

    es.indices.refresh(index="test-products")
