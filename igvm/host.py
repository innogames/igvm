"""igvm - Host Model

Copyright (c) 2018, InnoGames GmbH
"""

from StringIO import StringIO

import fabric.api
import fabric.state
from fabric.contrib import files
from uuid import uuid4

from paramiko import transport
from igvm.exceptions import RemoteCommandError
from igvm.settings import COMMON_FABRIC_SETTINGS


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
        if dataset_obj['hostname'].endswith('.ig.local'):
            self.fqdn = dataset_obj['hostname']
        else:
            self.fqdn = dataset_obj['hostname'] + '.ig.local'

    def __str__(self):
        return self.fqdn

    def __eq__(self, other):
        return isinstance(other, Host) and self.fqdn == other.fqdn

    def fabric_settings(self, *args, **kwargs):
        """Builds a fabric context manager to run commands on this host."""
        settings = COMMON_FABRIC_SETTINGS.copy()
        settings.update({
            'abort_exception': RemoteCommandError,
            'host_string': str(self.dataset_obj['intern_ip']),
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
            fd = StringIO()
            fabric.api.get(path, fd)
            return fd.getvalue()

    def put(self, remote_path, local_path, mode='0644'):
        """Same as Fabric's put but with working sudo permissions

        Setting permissions on files and using sudo via Fabric's put() seems
        broken, at least for mounted VM. This is why we run extra commands here.
        """
        with self.fabric_settings():
            tempfile = '/tmp/' + str(uuid4())
            fabric.api.put(local_path, tempfile)
            self.run('mv {0} {1} ; chmod {2} {1}'.format(
                tempfile, remote_path, mode
            ))
