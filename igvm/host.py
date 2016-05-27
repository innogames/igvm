import fabric.api

from adminapi.dataset import query, DatasetError, ServerObject


# TODO: Inherit from IGVMError
class RemoteCommandError(Exception):
    pass


def get_server(hostname, servertype=None):
    """Get a server from admintool by hostname

    Optionally check the servertype of the server.  Return the adminapi
    Server object."""

    # We want to return the server only, if matches with some conditions,
    # but we are not using those conditions on the query to give better errors.
    servers = tuple(query(hostname=hostname))

    if not servers:
        raise Exception('Server "{0}" not found.'.format(hostname))

    # Hostnames are unique on the serveradmin.  The query cannot return more
    # than one server.
    assert len(servers) == 1
    server = servers[0]

    if servertype and server['servertype'] != servertype:
        raise Exception('Server "{0}" is not a "{1}".'.format(
                hostname,
                servertype,
            ))

    if server.get('state') == 'retired':
        raise Exception('Server "{0}" is retired.'.format(hostname))

    return server


class Host(object):
    """A remote host on which commands can be executed."""

    def __init__(self, server_object, servertype=None):
        # Support passing hostname or admintool object.
        if not isinstance(server_object, ServerObject):
            server_object = get_server(server_object, servertype)

        self.hostname = server_object['hostname']
        self.admintool = server_object

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
        with fabric.api.settings(
                *settings,
                abort_exception=RemoteCommandError,
                host_string=self.hostname,
                warn_only=warn_only
            ):
            return fabric.api.run(*args, **kwargs)

