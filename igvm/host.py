from StringIO import StringIO

from adminapi.dataset import query, ServerObject
from adminapi.dataset.filters import ExactMatch, Startswith, Or

import fabric.api
import fabric.state
from fabric.contrib import files

from paramiko import transport
from igvm.exceptions import ConfigError, RemoteCommandError, InvalidStateError
from igvm.settings import COMMON_FABRIC_SETTINGS
from igvm.utils.lazy_property import lazy_property
from igvm.utils.network import get_network_config


def get_server(hostname, servertype):
    """Get a server from Serveradmin by hostname and servertype

    The function is accepting hostnames in any length as long as it resolves
    to a single server on Serveradmin.  It returns the adminapi Server object.
    """

    conditions = [ExactMatch(hostname)]
    if hostname.endswith('.ig.local'):
        conditions.append(ExactMatch(hostname[:-len('.ig.local')]))
    else:
        conditions.append(Startswith(hostname + '.'))

    # We want to return the server, only if it has the correct servertype,
    # but we are not using it as a condition on the query to be able to give
    # better errors.
    servers = list(query(hostname=Or(*conditions)))

    if not servers:
        raise ConfigError(
            'Server with hostname "{}" is not found.'.format(hostname)
        )

    server = servers[0]
    for other_server in servers[1:]:
        if other_server['servertype'] != servertype:
            continue
        if server['servertype'] != servertype:
            server = other_server
            continue

        raise ConfigError(
            'Hostname "{}" matches with multiple servers "{}" and "{}".'
            .format(hostname, server['hostname'], other_server['hostname'])
        )

    if server['servertype'] != servertype:
        raise ConfigError(
            'Server "{0}" is not a "{1}".'.format(hostname, servertype)
        )

    return server


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

    def __init__(self, server_name_or_obj, ignore_reserved=False):
        if isinstance(server_name_or_obj, ServerObject):
            self.server_obj = server_name_or_obj
        else:
            self.server_obj = get_server(server_name_or_obj, self.servertype)

        if self.server_obj['hostname'].endswith('.ig.local'):
            self.fqdn = self.server_obj['hostname']
        else:
            self.fqdn = self.server_obj['hostname'] + '.ig.local'

        if (
            not ignore_reserved and
            self.server_obj['state'] == 'online_reserved'
        ):
            raise InvalidStateError(
                'Server "{0}" is online_reserved.'.format(self.fqdn)
            )

    def fabric_settings(self, *args, **kwargs):
        """Builds a fabric context manager to run commands on this host."""
        settings = COMMON_FABRIC_SETTINGS.copy()
        settings.update({
            'abort_exception': RemoteCommandError,
            'host_string': str(self.server_obj['intern_ip']),
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

    def reload(self):
        """Reloads the server object from serveradmin."""
        if self.server_obj.is_dirty():
            raise ConfigError(
                'Serveradmin object must be committed before reloading'
            )
        self.server_obj = get_server(
            self.server_obj['hostname'], self.servertype
        )

    @lazy_property  # Requires fabric call on hypervisor, evaluate lazily.
    def network_config(self):
        """Returns networking attributes, such as IP address and VLAN."""
        return get_network_config(self.server_obj)

    @lazy_property
    def num_cpus(self):
        """Returns the number of online CPUs"""
        return int(self.run(
            'grep vendor_id < /proc/cpuinfo | wc -l',
            silent=True,
        ))
