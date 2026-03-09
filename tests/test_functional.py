import unittest

from keboola.datadirtest import DataDirTester
from keboola.datadirtest.datadirtest import TestDataDir
from freezegun import freeze_time


class TestDataDirWithState(TestDataDir):
    def _override_input_state(self, input_state: dict):
        if input_state is not None:
            super()._override_input_state(input_state)


class TestFunctional(unittest.TestCase):
    @freeze_time("2024-01-01")
    def test_functional(self):
        functional_tests = DataDirTester(test_data_dir_class=TestDataDirWithState)
        functional_tests.run()


if __name__ == "__main__":
    unittest.main()
