import unittest
from datadirtest import DataDirTester, TestDataDir
from freezegun import freeze_time
from elasticsearch import Elasticsearch
from faker import Faker
import os

try:
    from component import Component
except ModuleNotFoundError:
    from src.component import Component


class CustomDatadirTest(TestDataDir):

    def setUp(self):
        host = os.getenv("ELASTICSEARCH_HOST")
        if not host:
            host = "host.docker.internal"
        print(f"Connecting to host: {host}")
        ELASTICSEARCH_HOSTS = [{'host': host, 'port': 9200, 'scheme': 'http'}]
        INDEX_NAME = 'myindex'
        NUM_RECORDS = 10
        ELASTICSEARCH_USERNAME = 'elastic'
        ELASTICSEARCH_PASSWORD = 'root'

        inserter = ElasticSearchDataInserter(
            ELASTICSEARCH_HOSTS, INDEX_NAME, ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD
        )
        inserter.delete_all_records()
        inserter.insert_records(NUM_RECORDS)

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
        return {
            "id": self.fake.uuid4(),
            "state": {
                "id": self.fake.uuid4(),
                "text": self.fake.word()
            },
            "user": {
                "id": self.fake.uuid4(),
                "gender": self.fake.word(),
                "email": self.fake.email()
            },
            "address": {
                "zip": self.fake.zipcode(),
                "name": self.fake.first_name(),
                "surname": self.fake.last_name(),
                "street": self.fake.street_name(),
                "number": self.fake.building_number(),
                "city": self.fake.city()
            },
            "price": {
                "total_price": self.fake.random_number(),
                "total_price_cz": self.fake.random_number(),
                "payment_price": self.fake.random_number(),
                "shipment_price": self.fake.random_number(),
                "total_deal_price": self.fake.random_number(),
                "total_deal_purchase_price": self.fake.random_number(),
                "total_deal_sale_price": self.fake.random_number(),
                "total_deal_sale": self.fake.random_number()
            },
            "payment_type": {
                "id": self.fake.uuid4(),
                "name": self.fake.word()
            },
            "credit": self.fake.word(),
            "credit_payment": self.fake.word(),
            "shipment": {
                "id": self.fake.uuid4(),
                "total_price": self.fake.random_number(),
                "shipment_sid": self.fake.uuid4(),
                "shipment_name": self.fake.word(),
                "branch_id": self.fake.uuid4(),
                "branch_name": self.fake.word()
            },
            "currency": self.fake.word(),
            "description": self.fake.text(),
            "is_test": self.fake.word(),
            "source": self.fake.word(),
            "created_at": self.fake.date_time_this_month().isoformat(),
            "updated_at": self.fake.date_time_this_month().isoformat(),
            "ordered_at": self.fake.date_time_this_month().isoformat(),
            "done_at": self.fake.date_time_this_month().isoformat(),
            "payed": self.fake.word()
        }

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
