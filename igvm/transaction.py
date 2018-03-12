"""igvm - Transaction System

Copyright (c) 2018, InnoGames GmbH
"""

import logging

log = logging.getLogger(__name__)


class Transaction(object):
    """Context of an igvm action with rollback support.
    Each successful step register a callback to undo its changes.
    If the transaction fails, all registered callbacks are invoked in
    LIFO order."""
    def __init__(self):
        self._actions = None

    def __enter__(self):
        assert self._actions is None
        self._actions = []

        return self

    def __exit__(self, type, value, traceback):
        if traceback:
            self.rollback()

        assert self._actions is not None
        self._actions = None    # Invalidate transaction

    def on_rollback(self, name, fn, *args, **kwargs):
        assert callable(fn)

        self._actions.append((name, fn, args, kwargs))

    def rollback(self):
        log.info('Rolling back transaction')
        while self._actions:
            name, fn, args, kwargs = self._actions.pop()
            log.debug('Running rollback action "{}"'.format(name))

            try:
                fn(*args, **kwargs)
            except Exception as exception:
                log.warning(
                    'Rollback action "{}" failed: {}'.format(name, exception)
                )

    def checkpoint(self):
        """Mark a safe state within the transaction

        All previous on_rollback actions will not be invoked, even if
        the transaction fails later on.
        """
        log.debug('Checkpoint reached, all previous actions are now permanent')
        self._actions = []


def wrap_in_transaction(fn):
    def wrapped(*args, **kwargs):
        if kwargs.get('tx'):
            return fn(*args, **kwargs)

        with Transaction() as tx:
            kwargs['tx'] = tx
            return fn(*args, **kwargs)
    wrapped.__name__ = '{}_transaction'.format(fn.__name__)
    wrapped.__doc__ = fn.__doc__
    return wrapped


def run_in_transaction(fn):
    return wrap_in_transaction(fn)
