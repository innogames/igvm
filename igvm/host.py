"""igvm - Host Model

Copyright (c) 2018 InnoGames GmbH
"""

import logging
import shlex
import socket
from datetime import datetime, timezone
from io import BytesIO
from uuid import uuid4

import paramiko.ssh_exception

from igvm.exceptions import RemoteCommandError, InvalidStateError
from igvm.fabric_compat import (
    Connection,
    IS_LEGACY_FABRIC,
    make_fabric_config,
    _active_connections,
)
from igvm.settings import (
    FABRIC_CONNECTION_ATTEMPTS,
    FABRIC_CONNECTION_DEFAULTS,
    FABRIC_RUN_DEFAULTS,
)

from adminapi.dataset import DatasetError

log = logging.getLogger(__name__)


class CommandResult(str):
    """str subclass wrapping invoke.Result for backward compat: callers keep
    using .strip()/int()/`in`, plus .ok/.failed/.return_code/.stdout/.stderr.
    Fabric 1.x returned a string-like object; Fabric 3.x returns invoke.Result.
    """

    def __new__(cls, result):
        # Fabric 1.x stripped trailing whitespace; 3.x doesn't.  Strip here so
        # interpolated output (e.g. mktemp as a mount path) has no newline.
        stdout = result.stdout.strip() if result.stdout else ''
        instance = super().__new__(cls, stdout)
        instance._result = result
        return instance

    @property
    def return_code(self):
        return self._result.return_code

    @property
    def ok(self):
        return self._result.ok

    @property
    def failed(self):
        return self._result.failed

    @property
    def stdout(self):
        return self._result.stdout if self._result.stdout else ''

    @property
    def stderr(self):
        return self._result.stderr if self._result.stderr else ''


class Host(object):
    """A remote host on which commands can be executed."""

    def __init__(self, dataset_obj):
        self.dataset_obj = dataset_obj
        self.route_network = dataset_obj['route_network']['hostname']
        self.fqdn = self.dataset_obj['hostname']    # TODO: Remove
        self._connection = None

    def __str__(self):
        return self.fqdn

    def __hash__(self):
        return hash(self.fqdn)

    def __eq__(self, other):
        return isinstance(other, Host) and self.fqdn == other.fqdn

    @property
    def uid_name(self):
        """Name readable by both humans and machines

        Don't just assume this is the current domain or lv name of a VM. The
        whole point of these names is that hostnames may change while
        object_id may not.
        """
        return '{}_{}'.format(
            self.dataset_obj['object_id'],
            self.dataset_obj['hostname'])

    def match_uid_name(self, uid_name):
        """Check if a given uid_name matches this host"""
        return uid_name.split('_', 1)[0] == str(self.dataset_obj['object_id'])

    def _get_connection(self):
        """Return a cached fabric.Connection, built on first use.  fabric opens
        it lazily (run/sudo/get/put are ``@opens``-decorated) and reuses it; a
        dropped connection is cleared to None by run()'s retry loop, so the
        next call rebuilds it."""
        if self._connection is None:
            hostname = str(self.dataset_obj['hostname'])
            self._connection = Connection(
                hostname, config=make_fabric_config(),
                **FABRIC_CONNECTION_DEFAULTS,
            )
            _active_connections.add(self._connection)
        return self._connection

    def close_connection(self):
        """Close and discard the cached connection."""
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass
            _active_connections.discard(self._connection)
            self._connection = None

    def run(self, *args, **kwargs):
        """Runs a command on the remote host.
        :param warn_only: If set, no exception is raised if the command fails
        :param silent: If set, no output is written for successful runs"""
        warn_only = kwargs.pop('warn_only', False)
        with_sudo = kwargs.pop('with_sudo', True)
        silent = kwargs.pop('silent', False)

        run_kwargs = dict(FABRIC_RUN_DEFAULTS)
        run_kwargs['pty'] = kwargs.pop('pty', run_kwargs.get('pty', True))
        # Force warn=True so invoke never raises UnexpectedExit; we raise our
        # own RemoteCommandError below instead.
        run_kwargs['warn'] = True

        if silent:
            run_kwargs['hide'] = True

        # shell_escape was Fabric 1.x-only; drop it.
        kwargs.pop('shell_escape', None)

        # Fabric 1.x shell=False ("don't add an extra /bin/sh -c") -> shell=''.
        # Modern fabric's SSH runner ignores 'shell' (the bash -c wrap below
        # does the real work); the fabric3 adapter maps '' back to shell=False.
        if 'shell' in kwargs:
            shell_val = kwargs.pop('shell')
            if shell_val is False:
                run_kwargs['shell'] = ''

        run_kwargs.update(kwargs)

        # Remove abort_exception if present (Fabric 1.x only)
        run_kwargs.pop('abort_exception', None)

        conn = self._get_connection()
        runner = conn.sudo if with_sudo else conn.run

        # Modern fabric's sudo() prepends "sudo -S -p '...'" with no shell, so
        # shell constructs (pipes, while, &&) break.  Wrap in bash -c to keep
        # them working; fabric3 already shell-wraps sudo, so skip it there.
        if with_sudo and args and not IS_LEGACY_FABRIC:
            cmd = args[0]
            args = (f"bash -c {shlex.quote(cmd)}",) + args[1:]

        for attempt in range(FABRIC_CONNECTION_ATTEMPTS):
            try:
                result = runner(*args, **run_kwargs)
                if not warn_only and result.failed:
                    raise RemoteCommandError(
                        f'Command failed with return code {result.return_code}: {args[0] if args else ""}'
                    )
                return CommandResult(result)
            except (
                paramiko.ssh_exception.SSHException,
                paramiko.ssh_exception.NoValidConnectionsError,
                socket.error,
                EOFError,
            ):
                if attempt < FABRIC_CONNECTION_ATTEMPTS - 1:
                    log.warning(
                        'Connection lost, retrying (attempt %d/%d)...',
                        attempt + 2, FABRIC_CONNECTION_ATTEMPTS,
                    )
                    self.close_connection()
                    conn = self._get_connection()
                    runner = conn.sudo if with_sudo else conn.run
                else:
                    raise

    def file_exists(self, path, *args, **kwargs):
        """Check if a file exists on the remote host."""
        conn = self._get_connection()
        result = conn.sudo(
            f'test -e {path}',
            warn=True, hide=True,
        )
        return result.ok

    def read_file(self, path):
        """Reads a file from the remote host and returns contents."""
        if '*' in path:
            raise ValueError('No globbing supported')
        conn = self._get_connection()
        fd = BytesIO()
        conn.get(path, fd)
        return fd.getvalue()

    def put(self, remote_path, local_path, mode='0644'):
        """Same as Fabric's put but with working sudo permissions

        Setting permissions on files and using sudo via Fabric's put() seems
        broken, at least for mounted VM.  This is why we run extra commands
        in here.
        """
        conn = self._get_connection()
        tempfile = '/tmp/' + str(uuid4())
        conn.put(local_path, tempfile)
        self.run(
            f'mv {tempfile} {remote_path} ; chmod {mode} {remote_path}'
        )

    def acquire_lock(self):
        if self.dataset_obj['igvm_locked'] is not None:
            raise InvalidStateError(
                'Server "{0}" is already being worked on by another igvm'
                .format(self.dataset_obj['hostname'])
            )

        self.dataset_obj['igvm_locked'] = datetime.now(timezone.utc)
        try:
            self.dataset_obj.commit()
        except DatasetError:
            raise InvalidStateError(
                'Server "{0}" is already being worked on by another igvm'
                .format(self.dataset_obj['hostname'])
            )

    def release_lock(self):
        self.dataset_obj['igvm_locked'] = None
        self.dataset_obj.commit()

    def get_block_size(self, device):
        device = self.run((
            'lsblk -n -s -o TYPE,KNAME {} | awk \'/disk/ {{print $2}}\''
        ).format(device))
        sys_path = '/sys/class/block/{}/queue/'.format(device)
        bs = int(self.read_file(sys_path + 'max_sectors_kb'))
        bs_hw = int(self.read_file(sys_path + 'max_hw_sectors_kb'))
        return min(bs, bs_hw)

    def set_block_size(self, device, bs_kib):
        """ Reduce maximum number of KiB allowed for FS to request from
            block layer

        This is required for DRBD storage migrations. During disk migration
        a DRBD will consist of devices on multiple hypervisors which might have
        different maximum block size. It was observed that during high IO
        requests were sometimes rejected by block layer causing filesystem to
        crash.
        """
        self.run(
            'echo {} > /sys/class/block/{}/queue/max_sectors_kb'
            .format(bs_kib, device)
        )
        self.run('sync')
