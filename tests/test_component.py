import unittest
import mock
import os
from freezegun import freeze_time
from pydantic import ValidationError

from component import Component
from configuration import Configuration


class TestComponent(unittest.TestCase):
    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()


class TestPitKeepAliveValidation(unittest.TestCase):
    _DB = {"hostname": "localhost", "port": 9200}

    def test_valid_keep_alive_accepted(self):
        for value in ("5m", "30s", "1h", "2d", "500ms"):
            cfg = Configuration(db=self._DB, search_method="search_after", pit_keep_alive=value)
            self.assertEqual(cfg.pit_keep_alive, value)

    def test_invalid_keep_alive_rejected_for_search_after(self):
        with self.assertRaises(ValidationError):
            Configuration(db=self._DB, search_method="search_after", pit_keep_alive="5 minutes")

    def test_keep_alive_whitespace_is_trimmed(self):
        cfg = Configuration(db=self._DB, search_method="search_after", pit_keep_alive="  5m  ")
        self.assertEqual(cfg.pit_keep_alive, "5m")

    def test_keep_alive_not_enforced_for_scroll(self):
        # The scroll path never reads pit_keep_alive, so an odd value must not break existing configs.
        cfg = Configuration(db=self._DB, search_method="scroll", pit_keep_alive="not-a-time")
        self.assertEqual(cfg.search_method.value, "scroll")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
