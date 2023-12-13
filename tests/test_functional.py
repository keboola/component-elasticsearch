import unittest
from datadirtest import DataDirTester, TestDataDir
from freezegun import freeze_time
from elasticsearch import Elasticsearch
from faker import Faker
import time

try:
    from component import Component
except ModuleNotFoundError:
    from src.component import Component


class CustomDatadirTest(TestDataDir):

    def setUp(self):
        host = "elasticsearch"
        ELASTICSEARCH_HOSTS = [{'host': host, 'port': 9200, 'scheme': 'http'}]
        INDEX_NAME = 'myindex'
        NUM_RECORDS = 1
        ELASTICSEARCH_USERNAME = 'elastic'
        ELASTICSEARCH_PASSWORD = 'root'

        inserter = ElasticSearchDataInserter(
            ELASTICSEARCH_HOSTS, INDEX_NAME, ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD
        )
        inserter.delete_all_records()
        inserter.insert_records(NUM_RECORDS)
        time.sleep(5)

        super().setUp()

    def run_component(self):
        super().run_component()


class TestComponent(unittest.TestCase):
    @freeze_time("2023-11-03 14:50:42.833622")
    def test_functional(self):
        functional_tests = DataDirTester(test_data_dir_class=CustomDatadirTest)
        functional_tests.run()


class ElasticSearchDataInserter:

    def __init__(self, hosts, index_name, username, password):
        self.es = Elasticsearch(
            hosts=hosts,
            http_auth=(username, password)
        )
        # self.verify_connection()
        self.index_name = index_name
        self.fake = Faker()
        Faker.seed(0)

    def verify_connection(self):
        if not self.es.ping():
            raise ValueError("Connection to Elasticsearch failed!")

    def generate_random_document(self):
        return {"id": self.fake.uuid4()}

    def insert_records(self, num_records):
        for _ in range(num_records):
            document = self.generate_random_document()
            self.es.index(index=self.index_name, body=document)

    def delete_all_records(self):
        self.es.indices.create(index=self.index_name, ignore=400)
        query = {"query": {"match_all": {}}}
        self.es.delete_by_query(index=self.index_name, body=query)


if __name__ == "__main__":
    unittest.main()
