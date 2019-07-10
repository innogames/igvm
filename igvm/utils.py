"""igvm -  Utilities

Copyright (c) 2018 InnoGames GmbH
"""

from __future__ import division

import logging
import socket
import time
from os import path

from paramiko import SSHConfig

from igvm.exceptions import TimeoutError


_SIZE_FACTORS = {
    'T': 1024 ** 4,
    'G': 1024 ** 3,
    'M': 1024 ** 2,
    'K': 1024 ** 1,
    'B': 1024 ** 0,
}

log = logging.getLogger(__name__)


class LazyCompare(object):
    """Lazily execute the given function to compare its result"""
    def __init__(self, func, *args):
        self.func = func
        self.args = args
        self.executed = False
        self.result = None

    def __lt__(self, other):
        return self.sort_key() < other.sort_key()

    def __le__(self, other):
        return self.sort_key() <= other.sort_key()

    def __eq__(self, other):
        return self.sort_key() == other.sort_key()

    def __ge__(self, other):
        return self.sort_key() >= other.sort_key()

    def __gt__(self, other):
        return self.sort_key() > other.sort_key()

    def sort_key(self):
        if not self.executed:
            self.executed = True
            self.result = self.func(*self.args)
        return self.result


def retry_wait_backoff(fn_check, fail_msg, max_wait=20):
    """Continuously checks a conditional callback and retries with
    exponential backoff intervals until the condition is true.

    :param fn_check: Callable that return True if the condition holds
    :param fail_msg: Log message in case of failure, without trailing
                     punctuation, e.g. "Server is not online"
    :param wax_wait: Maximum total wait time. TimeoutError is raised if
                     max_wait expires."""
    sleep_time = 0.1
    total_waited = 0.0
    while total_waited < max_wait:
        if fn_check():
            break
        log.info('{0}, retrying in {1:.2f}s'.format(fail_msg, sleep_time))
        total_waited += sleep_time
        time.sleep(sleep_time)
        sleep_time = min(sleep_time * 2, 5, max_wait - total_waited)
    else:
        raise TimeoutError('{0} after {1:.2f}s'.format(fail_msg, max_wait))


def ping_port(ip, port=22, timeout=1):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((ip, port))
    except (socket.timeout, socket.error):
        return False
    else:
        return True
    finally:
        s.close()


def wait_until(ip, port=22, timeout=60, waitmsg=None):
    if waitmsg:
        log.info(waitmsg)

    for sec in range(timeout):
        if ping_port(ip, port):
            log.info('Success')
            return True

        if waitmsg:
            log.info('Remaining: {0} secs'.format(timeout - sec))
        time.sleep(1)

    return False


def parse_size(text, unit):
    """Return the size as integer in the desired unit.

    The TiB/GiB/MiB/KiB prefix is allowed as long as long as not ambiguous.
    We are dealing with the units case in-sensitively.
    """

    text = text.strip()
    text = text.upper()
    unit = unit.upper()

    # First, handle the suffixes
    if text.endswith('B'):
        text = text[:-1]
        if text.endswith('I'):
            text = text[:-1]

    if not text:
        return ValueError('Empty size')

    if text[-1] in _SIZE_FACTORS:
        factor = _SIZE_FACTORS[text[-1]]
        text = text[:-1]
    else:
        factor = _SIZE_FACTORS[unit]

    try:
        value = float(text) * factor
    except ValueError:
        raise ValueError(
            'Cannot parse "{}" as {}iB value.'.format(text, unit)
        )

    if value % _SIZE_FACTORS[unit]:
        raise ValueError('Value must be multiple of 1 {}iB'.format(unit))
    return int(value / _SIZE_FACTORS[unit])


def convert_size(size, from_name, to_name):
    return size / (
        _SIZE_FACTORS[from_name.upper()] * _SIZE_FACTORS[to_name.upper()]
    )


def get_ssh_config(hostname):
    """Get SSH config for given hostname

    :param: hostname: hostname

    :return: dict
    """

    ssh_config_file = path.abspath(path.expanduser('~/.ssh/config'))
    if path.exists(ssh_config_file):
        ssh_config = SSHConfig()
        with open(ssh_config_file) as f:
            ssh_config.parse(f)
            return ssh_config.lookup(hostname)

    return dict()
