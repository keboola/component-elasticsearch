import csv
import json
import logging
import os
import sys
from typing import List


class Writer:

    def __init__(self, path: str, table_name: str, incremental: bool = False, primary_keys: List = []):

        if (_tn := table_name.strip()) == '':
            logging.error("No table name provided.")
            sys.exit(1)
        else:
            self.table_name = _tn

        self.incremental = bool(incremental)
        self.table_path = path

        if isinstance(primary_keys, List) is False:
            logging.error("Primary keys must be provided as an array.")
            sys.exit(1)
        else:
            self.primary_keys = primary_keys

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

    def create_manifest(self, columns, incremental, primary_keys):

        with open(os.path.join(self.table_path, self.table_name) + '.manifest', 'w') as man_file:

            json.dump(
                {
                    'columns': columns,
                    'incremental': incremental,
                    'primary_key': primary_keys
                }, man_file
            )

    def _write_results(self, results):

        logging.info("Parsing results.")

        available_columns = []
        for res in results:
            for key in res.keys():
                if key not in available_columns:
                    available_columns += [key]
                else:
                    pass

        with open(os.path.join(self.table_path, self.table_name), 'w') as csv_out:

            _writer = csv.DictWriter(csv_out, fieldnames=available_columns, extrasaction='ignore',
                                     restval='', quoting=csv.QUOTE_ALL, quotechar='\"')

            _writer.writerows(results)

        return available_columns

    def write_results(self, results):

        _columns = self._write_results(results)
        self.create_manifest(_columns, self.incremental, self.primary_keys)
