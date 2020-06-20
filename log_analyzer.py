import re
import os
import sys
import json
import gzip
import logging
from string import Template
from argparse import ArgumentParser
from collections import defaultdict, namedtuple

from typing import Iterable, Iterator, NamedTuple

CONFIG = {
    'REPORT_SIZE': 100,
    'REPORT_DIR': './reports',
    'LOG_DIR': './logs',
    'PARSED_PERCENTS': 70,
}

REPORT_TEMPLATE_PATH = './report.html'

LOGFILE_PATTERN = re.compile(r'nginx-access-ui.log-(\d{8})(.gz)?')
ADDRESS_PATTERN = re.compile(r'\B(?:/(?:[\w?=_&-]+))+')
TIME_PATTERN = re.compile(r'\d+\.\d+$')


def parse_console_args():
    parser = ArgumentParser()
    parser.add_argument("--config", dest='config_file',
                        default='log_analyzer.json', type=str)

    return parser.parse_args()


def read_config_file(config: dict, config_file_path: str) -> dict:
    if config_file_path is not None:
        with open(config_file_path) as config_file:
            config_form_file = json.load(config_file)
            config.update(config_form_file)
    return config


def find_log_file(log_files: Iterable, log_dir: str) -> NamedTuple:

    log_files = sorted(log_files, reverse=True)

    new_log_file = ''
    date = None
    is_gzipped = None
    for f in log_files:
        filename = re.match(LOGFILE_PATTERN, f)
        if filename:
            new_log_file = f
            date = filename.group(1)
            is_gzipped = filename.group(2) is not None
            break
    if not new_log_file:
        logging.info('There are no log files in the directory')
        return None
    log_file_path = os.path.join(log_dir, new_log_file)

    Logfile = namedtuple('Logfile', ['path', 'date', 'gzipped'])
    actual_log_file = Logfile(log_file_path, date, is_gzipped)

    return actual_log_file


def generate_new_report_filename(report_dir: str, date: str) -> str:
    report_name = f'report-{date}.html'
    report_filename = os.path.join(report_dir, report_name)
    return report_filename


def is_report_existing(report_filename: str) -> bool:
    if os.path.exists(report_filename):
        logging.info('Your report is already in the report folder')
        return True
    else:
        return False


def read_log_file(log_file_destination: str, gzipped: bool) -> Iterator:
    log_file = open(log_file_destination, 'rb') if not gzipped else gzip.open(log_file_destination, 'rb')
    with log_file:
        for line in log_file:
            if line:
                yield line.decode('utf-8')


def parse_line(line: str) -> tuple:
    match_time = re.findall(TIME_PATTERN, line)
    match_address = re.findall(ADDRESS_PATTERN, line)
    if match_address:
        return (match_address[0],
                match_time[0])
    return None


def median(lst: Iterable) -> float:
    n = len(lst)
    if n < 1:
        return None
    if n % 2 == 1:
        return sorted(lst)[n // 2]
    return sum(sorted(lst)[n // 2 - 1:n // 2 + 1]) / 2.0


def aggregate_logs(log_iterator: Iterable, parsed_percent_from_config: int) -> list:
    logging.info('Aggregating raw data...')
    log_statistics = defaultdict(list)
    count_all = 0
    time_all = 0
    processed = 0

    for line in log_iterator:
        processed += 1
        parsed_line = parse_line(line)
        if parsed_line:
            url, time_opened = parsed_line
            count_all += 1
            time_all += float(time_opened)
        else:
            continue
        log_statistics[url].append(float(time_opened))

        if processed % 10000 == 0:
            logging.debug(f'Parsed {processed} lines')

    parsed_percent = count_all * 100 / processed
    logging.info(f'{parsed_percent}% of log lines are parsed')
    if parsed_percent < parsed_percent_from_config:
        raise RuntimeError('Fatal problem in log file')

    logging.info('Recalculating aggregated table...')

    result_table = []
    processed = 0
    for url in log_statistics:
        processed += 1
        times = log_statistics[url]
        counts = len(log_statistics[url])
        line = {
            'count': counts,
            'time_avg': round(sum(times) / counts, 3),
            'time_max': round(max(times), 3),
            'time_sum': round(sum(times), 3),
            'url': url,
            'time_med': round(median(times), 3),
            'time_perc': round(sum(times) * 100 / time_all, 3),
            'count_perc': round(counts * 100 / count_all, 3)
        }
        if processed % 10000 == 0:
            logging.debug(f'Calculated {processed} lines')
        result_table.append(line)

    return result_table


def generate_report_from_template(result_table: list, destination: str, report_size: int) -> None:
    sorted_result_json = json.dumps(sorted(result_table,
                                           key=lambda k: k['time_sum'],
                                           reverse=True)[:report_size])
    logging.info('Json report created')

    with open(REPORT_TEMPLATE_PATH, 'r') as html_template:
        template_data = Template(html_template.read())
    report_data = template_data.safe_substitute(table_json=sorted_result_json)

    with open(destination, 'w') as html_report:
        html_report.write(report_data)
    logging.info(f'HTML report is written to: {destination}')


def main(config: dict) -> None:
    # read all files in directory and check that logs exists
    try:
        log_files = os.listdir(CONFIG['LOG_DIR'])
    except OSError:
        logging.error('Can`t find directory for logs.')
        sys.exit()

    actual_log_file = find_log_file(log_files, config['LOG_DIR'])

    if not actual_log_file:
        sys.exit()

    if not os.path.exists(config['REPORT_DIR']):
        os.mkdir(config['REPORT_DIR'])

    report_path = generate_new_report_filename(config['REPORT_DIR'], actual_log_file.date)

    # check if report exists
    if is_report_existing(report_path):
        sys.exit()

    # parsing and aggregate raw data from log file
    log_iterator = read_log_file(actual_log_file.path, actual_log_file.gzipped)
    result_table = aggregate_logs(log_iterator, config['PARSED_PERCENTS'])

    generate_report_from_template(result_table, report_path, config['REPORT_SIZE'])


if __name__ == "__main__":

    args = parse_console_args()
    config_destination = args.config_file

    conf = read_config_file(CONFIG, config_destination)

    logging.basicConfig(filename=conf.get('MONITORING_LOG', None),
                        level=logging.INFO,
                        format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')

    logging.info('Logging started')

    try:
        main(config=conf)
    except Exception as e:
        logging.exception(e)
