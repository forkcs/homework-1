import unittest

from log_analyzer import *


class SimpleFunctionsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.CONFIG = {
            'REPORT_SIZE': 100,
            'REPORT_DIR': './reports',
            'LOG_DIR': './logs',
            'PARSED_PERCENTS': 70,
        }

    def test_read_config_file(self):
        new_conf = read_config_file(config=self.CONFIG, config_file_path=None)
        self.assertEqual(self.CONFIG, new_conf)
