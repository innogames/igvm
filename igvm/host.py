from StringIO import StringIO

from adminapi.dataset import query, ServerObject

import fabric.api
import fabric.state

from igvm.exceptions import ConfigError, RemoteCommandError
from igvm.settings import COMMON_FABRIC_SETTINGS
from igvm.utils.lazy_property import lazy_property
from igvm.utils.network import get_network_config


def get_server(hostname, servertype):
    """Get a server from Serveradmin by hostname and servertype

    It returns the adminapi Server object.
    """

    # We want to return the server only, if matches with some conditions,
    # but we are not using those conditions on the query to give better errors.
    servers = tuple(query(hostname=hostname))

    if not servers:
        raise ConfigError('Server "{0}" not found.'.format(hostname))

    # Hostnames are unique on the serveradmin.  The query cannot return more
    # than one server.
    assert len(servers) == 1
    server = servers[0]

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

    def __init__(self, server_object):
        # Support passing hostname or Server object.
        if not isinstance(server_object, ServerObject):
            server_object = get_server(server_object, self.servertype)

        self.hostname = server_object['hostname']
        self.admintool = server_object
        if self.hostname.endswith('.ig.local'):
            self.fqdn = self.hostname
        else:
            self.fqdn = self.hostname + '.ig.local'

    def fabric_settings(self, *args, **kwargs):
        """Builds a fabric context manager to run commands on this host."""
        settings = COMMON_FABRIC_SETTINGS.copy()
        settings.update({
            'abort_exception': RemoteCommandError,
            'host_string': str(self.admintool['intern_ip']),
        })
        settings.update(kwargs)
        return fabric.api.settings(*args, **settings)

    def run(self, *args, **kwargs):
        """Runs a command on the remote host.
        :param warn_only: If set, no exception is raised if the command fails
        :param silent: If set, no output is written for successful runs"""
        settings = []
        warn_only = kwargs.get('warn_only', False)
        if kwargs.get('silent', False):
            hide = 'everything' if warn_only else 'commands'
            settings.append(fabric.api.hide(hide))

        # Purge settings that should not be passed to run()
        for setting in ['warn_only', 'silent']:
            if setting in kwargs:
                del kwargs[setting]

        with self.fabric_settings(*settings, warn_only=warn_only):
            return fabric.api.run(*args, **kwargs)

    def read_file(self, path):
        """Reads a file from the remote host and returns contents."""
        if '*' in path:
            raise ValueError('No globbing supported')
        with self.fabric_settings(fabric.api.hide('commands')):
            fd = StringIO()
            fabric.api.get(path, fd)
            return fd.getvalue()

    def disconnect(self):
        """Disconnect active Fabric sessions."""
        if self.hostname in fabric.state.connections:
            fabric.state.connections[self.hostname].get_transport().close()

    def reload(self):
        """Reloads the server object from serveradmin."""
        if self.admintool.is_dirty():
            raise ConfigError(
                'Server object must be committed before reloading'
            )
        self.admintool = get_server(self.hostname, self.servertype)

    @lazy_property  # Requires fabric call on HV, evaluate lazily.
    def network_config(self):
        """Returns networking attributes, such as IP address and VLAN."""
        return get_network_config(self.admintool)

    @lazy_property
    def num_cpus(self):
        """Returns the number of online CPUs"""
        return int(self.run(
            'grep vendor_id < /proc/cpuinfo | wc -l',
            silent=True,
        ))

    def accept_ssh_hostkey(self, dst_host):
        """Scans and accepts the SSH remote host key of a given host.
        NO VERIFICATION IS PERFORMED, THIS IS INSECURE!"""
        self.run('touch .ssh/known_hosts'.format(dst_host.hostname))
        self.run('ssh-keygen -R {0}'.format(dst_host.hostname))
        self.run(
            'ssh-keyscan -t rsa {0} >> .ssh/known_hosts'
            .format(dst_host.hostname)
        )
