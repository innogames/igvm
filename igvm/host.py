"""igvm - Host Model

Copyright (c) 2018 InnoGames GmbH
"""

from io import BytesIO
from datetime import datetime

import fabric.api
import fabric.state
from fabric.contrib import files
from uuid import uuid4

from paramiko import transport
from igvm.exceptions import RemoteCommandError, InvalidStateError
from igvm.settings import COMMON_FABRIC_SETTINGS

from adminapi.dataset import DatasetError


def with_fabric_settings(fn):
    """Decorator to run a function with COMMON_FABRIC_SETTINGS."""
    def decorator(*args, **kwargs):
        with fabric.api.settings(**COMMON_FABRIC_SETTINGS):
            return fn(*args, **kwargs)
    decorator.__name__ = '{}_with_fabric'.format(fn.__name__)
    decorator.__doc__ = fn.__doc__
    return decorator


class Host(object):
    """A remote host on which commands can be executed."""

    def __init__(self, dataset_obj):
        self.dataset_obj = dataset_obj
        self.route_network = dataset_obj['route_network']['hostname']
        self.fqdn = self.dataset_obj['hostname']    # TODO: Remove

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

    def fabric_settings(self, *args, **kwargs):
        """Builds a fabric context manager to run commands on this host."""
        settings = COMMON_FABRIC_SETTINGS.copy()
        settings.update({
            'abort_exception': RemoteCommandError,
            'host_string': str(self.dataset_obj['hostname']),
        })
        settings.update(kwargs)
        return fabric.api.settings(*args, **settings)

    def run(self, *args, **kwargs):
        """Runs a command on the remote host.
        :param warn_only: If set, no exception is raised if the command fails
        :param silent: If set, no output is written for successful runs"""
        settings = []
        warn_only = kwargs.get('warn_only', False)
        with_sudo = kwargs.get('with_sudo', True)
        kwargs['pty'] = kwargs.get('pty', True)
        if kwargs.get('silent', False):
            hide = 'everything' if warn_only else 'commands'
            settings.append(fabric.api.hide(hide))

        # Purge settings that should not be passed to run()
        for setting in ['warn_only', 'silent', 'with_sudo']:
            if setting in kwargs:
                del kwargs[setting]

        with self.fabric_settings(*settings, warn_only=warn_only):
            try:
                if with_sudo:
                    return fabric.api.sudo(*args, **kwargs)
                else:
                    return fabric.api.run(*args, **kwargs)
            except transport.socket.error:
                # Retry once if connection was lost
                host = fabric.api.env.host_string
                if host and host in fabric.state.connections:
                    fabric.state.connections[host].get_transport().close()
                if with_sudo:
                    return fabric.api.sudo(*args, **kwargs)
                else:
                    return fabric.api.run(*args, **kwargs)

    def file_exists(self, *args, **kwargs):
        """Run a fabric.contrib.files.exists on this host with sudo."""
        with self.fabric_settings():
            try:
                return files.exists(*args, **kwargs)
            except transport.socket.error:
                # Retry once if connection was lost
                host = fabric.api.env.host_string
                if host and host in fabric.state.connections:
                    fabric.state.connections[host].get_transport().close()
                return files.exists(*args, **kwargs)

    def read_file(self, path):
        """Reads a file from the remote host and returns contents."""
        if '*' in path:
            raise ValueError('No globbing supported')
        with self.fabric_settings(fabric.api.hide('commands')):
            fd = BytesIO()
            fabric.api.get(path, fd)
            return fd.getvalue()

    def put(self, remote_path, local_path, mode='0644'):
        """Same as Fabric's put but with working sudo permissions

        Setting permissions on files and using sudo via Fabric's put() seems
        broken, at least for mounted VM.  This is why we run extra commands
        in here.
        """
        with self.fabric_settings():
            tempfile = '/tmp/' + str(uuid4())
            fabric.api.put(local_path, tempfile)
            self.run(
                'mv {0} {1} ; chmod {2} {1}'
                .format(tempfile, remote_path, mode)
            )

    def acquire_lock(self, allow_fail=False):
        if self.dataset_obj['igvm_locked'] is not None:
            raise InvalidStateError(
                'Server "{0}" is already being worked on by another igvm'
                .format(self.dataset_obj['hostname'])
            )

        self.dataset_obj['igvm_locked'] = datetime.utcnow()
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
