"""igvm - Exceptions

Copyright (c) 2018 InnoGames GmbH
"""


class IGVMError(Exception):
    pass


class ConfigError(IGVMError):
    """Indicates an error with the Serveradmin configuration."""
    pass


class HypervisorError(IGVMError):
    """Something went wrong on the hypervisor."""
    pass


class NetworkError(IGVMError):
    pass


class RemoteCommandError(IGVMError):
    """A command on the remote host failed."""
    pass


class StorageError(IGVMError):
    """Something related to storage went wrong."""
    pass


class VMError(IGVMError):
    """Something related to a VM went wrong."""
    pass


class InvalidStateError(IGVMError):
    """Host state is invalid for the requested operation."""
    pass


class MigrationError(IGVMError):
    """Indicates an error during migration."""
    pass


class MigrationAborted(MigrationError):
    """Indicates an error during migration."""
    pass


class TimeoutError(IGVMError):
    """An operation timed out."""
    pass

class IGVMTestError(IGVMError):
    """Indicates an error during tests."""
    pass

class InconsistentAttributeError(IGVMError):
    """An attribute on the VM differs from the excepted value from
    Serveradmin."""

    def __init__(self, vm, attribute, actual_value):
        self.fqdn = vm.fqdn
        self.attribute = attribute
        self.actual_value = actual_value
        self.config_value = vm.dataset_obj[attribute]
        assert self.config_value != self.actual_value

    def __str__(self):
        return (
            'Attribute "{}" on "{}" is out of sync: '
            '{} (config) != {} (actual)'.format(
                self.attribute,
                self.fqdn,
                self.config_value,
                self.actual_value,
            )
        )
