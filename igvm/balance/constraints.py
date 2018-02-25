"""igvm - Balancing Constraints

Copyright (c) 2018, InnoGames GmbH
"""


class Constraint(object):
    """Base Constraint Class

    This class is the base constraints class from which all concrete
    constraints should inherit to fulfil the base contract of how to access it.
    Within all classes the inherit from Constraint you have access to the
    following standard attributes as well as all key, values as attributes you
    attached in configuration.
    """

    def __init__(self, *args, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def fulfilled(self, vm, hv):
        """Constraint is fulfilled

        Return True if constraint is fulfilled or False if not.

        :param: vm: igvm.balance.models.VM object
        :param: hv: igvm.balance.models.Hypervisor object

        :return: bool
        """

        raise NotImplementedError()


class DiskSpace(Constraint):
    """Disk Space Constraint

    Check if enough disk space is free on target hypervisor. Disk space is
    determined by used disk space of VM.
    """

    def __init__(self, *args, **kwargs):
        super(DiskSpace, self).__init__(*args, **kwargs)

    def fulfilled(self, vm, hv):
        return hv.get_disk_free(fast=True) - self.reserved > vm.get_disk_size()


class Memory(Constraint):
    """Memory Constraint

    Check if hypervisor has enough memory free to move desired vm there
    """

    def __init__(self, *args, **kwargs):
        super(Memory, self).__init__(*args, **kwargs)

    def fulfilled(self, vm, hv):
        return hv.get_memory_free(fast=True) > vm.get_memory()


class EnsureFunctionDistribution(Constraint):
    """Game World / Function Distribution

    Ensure that redundant servers don't reside on the same hypervisor
    """

    def __init__(self, *args, **kwargs):
        super(EnsureFunctionDistribution, self).__init__(*args, **kwargs)

    def fulfilled(self, vm, hv):
        for hv_vm in hv.get_vms():
            if hv_vm.hostname == vm.hostname:
                continue
            if hv_vm.get_identifier() == vm.get_identifier():
                return False
        return True


class HypervisorMaxVcpuUsage(Constraint):
    """Hypervisor Max vCPU usage

    Checks the maximum vCPU usage (95 percentile) of the given hypervisor for
    the given time_range and dismisses it as target when it is over the value
    of threshold.
    """

    def __init__(self, *args, **kwargs):
        super(HypervisorMaxVcpuUsage, self).__init__(*args, **kwargs)

    def fulfilled(self, vm, hv):
        return hv.get_max_vcpu_usage() < self.threshold
