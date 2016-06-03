class IGVMError(Exception):
    pass


class ConfigError(IGVMError):
    """Indicates an error with the admintool configuration."""
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
