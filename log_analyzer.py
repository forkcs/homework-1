import re
import gzip
import json
import logging
import argparse

from typing import Generator, Iterable

from string import Template


config = {
    'REPORT_SIZE': 1000,
    'REPORT_DIR': './reports',
    'LOG_DIR': './log'
}

with open('report.html', 'r') as report_template:
    REPORT_TEMPLATE = report_template.read()


def get_last_log_filename(logs_dir: str) -> str:
    pass


def generate_report_dict(log_file: Iterable) -> dict:
    for line in log_file:
        pass


def generate_report(report: dict) -> str:
    report_json = json.dumps(report)
    report_html_template = Template(REPORT_TEMPLATE).safe_substitute(table_json=report_json)
    return report_html_template


def open_log_file(filename: str) -> Generator:
    if filename.endswith('.gz'):
        file = gzip.open(filename=filename, mode='rb')
    else:
        file = open(file=filename, mode='r')
    for line in file:
        yield line.decode()
    file.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(name='--config', nargs=1, dest='config', default=None)
    args = parser.parse_args()
    if args.config is not None:
        pass


if __name__ == '__main__':
    logging.basicConfig(filename=config['OWN_LOG_FILE'] or None,
                        level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')
    try:
        main()
    except Exception as e:
        logging.exception(e)
