import unittest

from datadirtest import DataDirTester
from freezegun import freeze_time


class TestFunctional(unittest.TestCase):
    @freeze_time("2024-01-01")
    def test_functional(self):
        functional_tests = DataDirTester()
        functional_tests.run()


if __name__ == "__main__":
    unittest.main()
