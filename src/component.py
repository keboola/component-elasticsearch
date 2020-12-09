import dateparser
import json
import logging
import sys

from dataclasses import dataclass
from client import SshClient
from result import Writer
from kbc.env_handler import KBCEnvHandler

COMPONENT_VERSION = '1.0.0'
sys.tracebacklimit = 0

KEY_INDEX_NAME = 'index_name'
KEY_REQUEST_BODY = 'request_body'
KEY_STORAGE_TABLE = 'storage_table'
KEY_INCREMENTAL = 'incremental'
KEY_PRIMARY_KEYS = 'primary_keys'

KEY_DATE = 'date'
KEY_DATE_APPEND = 'append_date'
KEY_DATE_FORMAT = 'format'
KEY_DATE_SHIFT = 'shift'

KEY_SSH = 'ssh'
KEY_SSH_USE = 'use_ssh'
KEY_SSH_HOST = 'hostname'
KEY_SSH_PORT = 'port'
KEY_SSH_USERNAME = 'username'
KEY_SSH_PKEY = '#private_key'

KEY_DB = 'db'
KEY_DB_HOST = 'hostname'
KEY_DB_PORT = 'port'

KEY_DEBUG = 'debug'

MANDATORY_PARAMS = [KEY_INDEX_NAME, KEY_DB, KEY_STORAGE_TABLE]


@dataclass
class SshTunnel:
    hostname: str
    port: int
    username: str
    key: str


@dataclass
class Database:
    host: str
    port: str


class Component(KBCEnvHandler):

    BATCH_PROCESSING_SIZE = 300000

    def __init__(self):

        super().__init__(mandatory_params=MANDATORY_PARAMS, log_level='INFO')

        logging.info(f"Running component version {COMPONENT_VERSION}.")

        if self.cfg_params.get('debug', False) is True:
            logger = logging.getLogger()
            logger.setLevel(level='DEBUG')

        try:
            self.validate_config(MANDATORY_PARAMS)

        except ValueError as e:
            logging.exception(e)
            sys.exit(1)

        _db_object = self._parse_db_parameters()
        _ssh_object = self._parse_ssh_parameters()
        self.index, self.index_params = self._parse_index_parameters()
        self.client = SshClient(_ssh_object, _db_object)
        self.writer = Writer(self.tables_out_path, self.cfg_params[KEY_STORAGE_TABLE],
                             self.cfg_params.get(KEY_INCREMENTAL, True),
                             self.cfg_params.get(KEY_PRIMARY_KEYS, []))

    def _parse_ssh_parameters(self):

        ssh_config = self.cfg_params.get(KEY_SSH, {})

        if ssh_config == {} or ssh_config.get(KEY_SSH_USE) is False:

            logging.info("SSH not specified or not enabled. Using standard method.")
            logging.error("Method not implemented.")
            sys.exit(2)

        else:

            try:
                ssh_object = SshTunnel(ssh_config[KEY_SSH_HOST], ssh_config[KEY_SSH_PORT],
                                       ssh_config[KEY_SSH_USERNAME], ssh_config[KEY_SSH_PKEY])

            except KeyError as e:
                logging.exception(f"Missing mandatory field {e} in SSH configuration.")
                sys.exit(1)

            return ssh_object

    def _parse_db_parameters(self):

        db_config = self.cfg_params[KEY_DB]

        try:
            db_object = Database(db_config[KEY_DB_HOST], db_config[KEY_DB_PORT])

        except KeyError as e:
            logging.exception(f"Missing mandatory field {e} in DB configuration.")
            sys.exit(1)

        return db_object

    def _parse_index_parameters(self):

        index = self.cfg_params[KEY_INDEX_NAME]
        date_config = self.cfg_params.get(KEY_DATE, {})

        _req_body = self.cfg_params.get(KEY_REQUEST_BODY, '{}').strip()
        request_body_string = _req_body if _req_body != '' else '{}'

        if date_config.get(KEY_DATE_APPEND, False) is True:

            _date = dateparser.parse(date_config.get(KEY_DATE_SHIFT, 'yesterday'))

            if _date is None:
                logging.error(f"Could not parse value {date_config[KEY_DATE_SHIFT]} to date.")
                sys.exit(1)

            _date_formatted = _date.strftime(date_config.get(KEY_DATE_FORMAT, '%Y-%m-%d'))

            if '{{date}}' in index:
                index = index.replace('{{date}}', _date_formatted)
                logging.info(f"Replaced date placeholder with value {_date_formatted}. " +
                             f"Downloading data from index {index}.")

            else:
                logging.warn(f"No date placeholder found in index name {index}.")

        try:
            request_body = json.loads(request_body_string)

        except ValueError:
            logging.exception("Could not parse request body string to JSON.")
            sys.exit

        return index, request_body

    def parse_curl_stdout(self, stdout):

        stdout_split = stdout.split('\r\n\r\n')

        rsp_status = stdout_split[0].split(' ')[1]
        rsp_body = stdout_split[1]

        return rsp_status, rsp_body

    def parse_scroll(self, scroll_response):

        try:
            scroll_json = json.loads(scroll_response)
        except ValueError as e:
            logging.exception(f"Could not parse JSON response - {e}.")
            sys.exit(1)

        return scroll_json['_scroll_id'], scroll_json['hits']['total'], scroll_json['hits']['hits']

    def run(self):

        all_results = []
        is_complete = False

        _fp_out, _fp_err = self.client.get_first_page(self.index, self.index_params)

        if _fp_out == '' and _fp_err != '':
            logging.error(f"Could not download data. Error: {_fp_err}")
            sys.exit(1)

        elif _fp_out == '' and _fp_err == '':
            logging.error("No data returned.")
            sys.exit(1)

        else:
            pass

        logging.debug("Parsing first page.")
        stdout_sc, stdout_body = self.parse_curl_stdout(_fp_out)

        if stdout_sc != '200':
            logging.error(f"Could not download data. Error: {stdout_body}.")
            sys.exit(1)

        else:
            _scroll_id, _nr_results, _results = self.parse_scroll(stdout_body)

        logging.info(f"{_nr_results} rows will be downloaded from index {self.index}.")
        all_results += [self.writer.flatten_json(r) for r in _results]

        if len(_results) < self.client.REQUEST_SIZE:
            is_complete = True

        already_written = 0
        while is_complete is False:

            _scroll_out, _scroll_err = self.client.get_scroll(_scroll_id)

            if _scroll_out == '':
                logging.error(f"Could not download data for scroll {_scroll_id}.\n" +
                              f"STDERR: {_scroll_err}.")
                sys.exit(1)

            else:
                pass

            stdout_sc, stdout_body = self.parse_curl_stdout(_scroll_out)

            if stdout_sc != '200':
                logging.error(f"Could not download data. Error: {stdout_body}.")
                sys.exit(1)

            else:
                _scroll_id, _, _results = self.parse_scroll(stdout_body)

            all_results += [self.writer.flatten_json(r) for r in _results]

            if len(_results) < self.client.REQUEST_SIZE:
                is_complete = True

            if ((_results_len := len(all_results)) >= self.BATCH_PROCESSING_SIZE) or is_complete is True:
                self.writer.write_results(all_results, is_complete=is_complete)
                all_results = []
                already_written += _results_len

                logging.debug(f"Parsed {already_written} results so far.")

        logging.info(f"Downloaded all data for index {self.index}. Parsed {already_written} rows.")
        self.writer.create_manifest(self.writer.result_schema, self.writer.incremental, self.writer.primary_keys)

        logging.info("Component finished.")
