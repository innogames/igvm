"""igvm -  Utilities

Copyright (c) 2018 InnoGames GmbH
"""

from __future__ import division

import logging
import os
import socket
import time

from fabric.api import puts
from fabric.contrib.files import upload_template as _upload_template

from igvm.exceptions import TimeoutError


_SIZE_FACTORS = {
    'T': 1024 ** 4,
    'G': 1024 ** 3,
    'M': 1024 ** 2,
    'K': 1024 ** 1,
    'B': 1024 ** 0,
}

log = logging.getLogger(__name__)


class ComparableByKey(object):
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


class LazyCompare(ComparableByKey):
    """Lazily execute the given function to compare its result"""
    def __init__(self, func, *args):
        self.func = func
        self.args = args
        self.executed = False
        self.result = None

    def sort_key(self):
        if not self.executed:
            self.executes = True
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
        puts(waitmsg)

    for sec in range(timeout):
        if ping_port(ip, port):
            puts('Success')
            return True

        if waitmsg:
            puts('Remaining: {0} secs'.format(timeout - sec))
        time.sleep(1)

    return False


def upload_template(filename, destination, context=None):
    template_dir = os.path.join(os.path.dirname(__file__), 'templates')

    _upload_template(
        filename,
        destination,
        context,
        backup=False,
        use_jinja=True,
        template_dir=template_dir,
        use_sudo=True,
    )


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
