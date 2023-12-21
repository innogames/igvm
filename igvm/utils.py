"""igvm -  Utilities

Copyright (c) 2018 InnoGames GmbH
"""

from __future__ import division

import json
import logging
import socket
import time
from concurrent import futures
from json import JSONDecodeError
from os import path
from pathlib import Path
from typing import Union, List
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from paramiko import SSHConfig

from igvm.exceptions import TimeoutError
from igvm.settings import (
    AWS_INSTANCES_OVERVIEW_URL,
    AWS_INSTANCES_OVERVIEW_FILE,
    AWS_INSTANCES_OVERVIEW_FILE_ETAG,
    AWS_FALLBACK_INSTANCE_TYPE
)

_SIZE_FACTORS = {
    'T': 1024 ** 4,
    'G': 1024 ** 3,
    'M': 1024 ** 2,
    'K': 1024 ** 1,
    'B': 1024 ** 0,
}

log = logging.getLogger(__name__)


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


def parallel(
    fn,
    workers=10,
    return_results=True,
    identifiers=None,
    args=None,
    kwargs=None,
):
    """Runs given function in separate threads for each argument/s given.

    By default this function is blocking and returns the results when all
    futures have resolved. If this is not desired then it is possible to get
    back the raw futures by settings return_results=False.

    It is possible to pass a list of identifiers that will be returned together
    with the results when return_results=True. This can be helpful if a result
    must be associated with something. Must be in the same order as args and
    kwargs, if any. The other possibility would be to let fn itself return
    some data structure that includes the necessary information.

    :param: fns: Functions to execute
    :param: workers: How many parallel worker threads
    :param: return_results: Whether to return only the results after execution
                            or to directly return the futures for more complex
                            scenarios without waiting for their execution
    :param: identifiers: List of identifiers that will be returned together
                         with the corresponding results in the form of a dict
    :param: args: List of lists to be passed as *args to each fn call
    :param: kwargs: List of dicts to be passed as **kwargs to each fn call
    """
    # Check user input
    if args is not None and kwargs is not None:
        err = 'Amount of args must match those of kwargs'
        assert len(args) == len(kwargs), err

    if (args is not None or kwargs is not None) and identifiers is not None:
        err = 'Amount of identifier must match those of kw/args'
        n_args = len(args) if args is not None else len(kwargs)
        assert n_args == len(identifiers), err

    # Preprocessing for arguments lists
    identifiers = [] if identifiers is None else identifiers
    args = [] if args is None else args
    kwargs = [] if kwargs is None else kwargs

    if len(args) == 0 and len(kwargs) == 0:
        args = [None]
        kwargs = [None]
    else:
        if len(args) == 0:
            args = [[] for _ in range(len(kwargs))]
        if len(kwargs) == 0:
            kwargs = [dict() for _ in range(len(args))]

    # Initialize all the futures
    executor = futures.ThreadPoolExecutor(max_workers=workers)
    _futures = [
        executor.submit(fn, *args[i], **kwargs[i])
        for i in range(len(args))
    ]

    # Return only futures when requested
    if not return_results:
        return _futures

    # Block until we received all results
    if len(identifiers) > 0:
        results = {}
    else:
        results = []

    for i, future in enumerate(_futures):
        result = future.result()

        if len(identifiers) > 0:
            results[identifiers[i]] = result
        else:
            results.append(result)

    return results


def aws_get_instances_overview(timeout: int = 5) -> Union[List, None]:
    """AWS Get Instances Overview

    Load or download the latest instances.json, which contains
    a complete overview about all instance_types, their configuration,
    performance and pricing.

    :param: timeout: Timeout value for the head/get request

    :return: VM types overview as list
             or None, if the parsing/download failed
    """

    url = AWS_INSTANCES_OVERVIEW_URL
    file = Path.home() / AWS_INSTANCES_OVERVIEW_FILE
    etag_file = Path.home() / AWS_INSTANCES_OVERVIEW_FILE_ETAG

    try:
        head_req = Request(url, method='HEAD')
        resp = urlopen(head_req, timeout=timeout)
        if resp.status == 200:
            etag = dict(resp.info())['ETag']
        else:
            log.warning('Could not retrieve ETag from {}'.format(url))
            etag = None
        if file.exists() and etag_file.exists() and etag:
            with open(etag_file, 'r+') as f:
                prev_etag = f.read()
            if etag == prev_etag:
                with open(file, 'r+') as f:
                    return json.load(f)

        resp = urlopen(url, timeout=timeout)
        if etag:
            with open(etag_file, 'w+') as f:
                f.write(etag)
        with open(file, 'w+') as f:
            content = resp.read().decode('utf-8')
            f.write(content)

            return json.loads(content)
    except (HTTPError, JSONDecodeError, URLError) as e:
        log.warning('Could not retrieve instances overview')
        log.warning(e)
        log.info('Proceeding with instance_type: '
                 f'{AWS_FALLBACK_INSTANCE_TYPE}'
        )

        return None
