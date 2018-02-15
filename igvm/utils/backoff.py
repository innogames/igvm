"""igvm - Backoff Utility

Copyright (c) 2018, InnoGames GmbH
"""

import logging
import time

from igvm.exceptions import TimeoutError


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
        sleep_time = min(sleep_time*2, 5, max_wait-total_waited)
    else:
        raise TimeoutError('{0} after {1:.2f}s'.format(fail_msg, max_wait))
