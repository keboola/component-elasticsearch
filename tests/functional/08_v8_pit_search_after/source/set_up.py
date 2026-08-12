from keboola.datadirtest import TestDataDir
from elasticsearch import Elasticsearch


def run(context: TestDataDir):
    es = Elasticsearch("http://elasticsearch8:9200")

    index = "test-audit-log"
    if es.indices.exists(index=index):
        es.indices.delete(index=index)

    # Single shard + explicit date mapping keeps the _shard_doc order deterministic (indexing order)
    # so the expected output is stable.
    es.indices.create(
        index=index,
        settings={"number_of_shards": 1},
        mappings={"properties": {"created_at": {"type": "date"}}},
    )

    # The first three documents deliberately share the same created_at value. With a page size of 2
    # and a sort on the non-unique created_at field, that tie spans a page boundary. This exercises
    # PIT + search_after pagination end to end and locks in that all five rows are extracted across
    # the boundary (Elasticsearch adds an implicit _shard_doc tiebreaker to every PIT search).
    documents = [
        {"id": 1, "created_at": "2024-01-01T00:00:00Z", "val": "a"},
        {"id": 2, "created_at": "2024-01-01T00:00:00Z", "val": "b"},
        {"id": 3, "created_at": "2024-01-01T00:00:00Z", "val": "c"},
        {"id": 4, "created_at": "2024-01-02T00:00:00Z", "val": "d"},
        {"id": 5, "created_at": "2024-01-03T00:00:00Z", "val": "e"},
    ]

    for doc in documents:
        es.index(index=index, id=doc["id"], document=doc)

    es.indices.refresh(index=index)
