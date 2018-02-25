"""igvm - Balancing Constraints

This module contains constraints to select hypervisors.

Copyright (c) 2018, InnoGames GmbH
"""


class DiskSpace(object):
    """Disk Space Constraint

    Check if enough disk space is free on target hypervisor. Disk space is
    determined by used disk space of VM.
    """
    def __init__(self, reserved):
        self.reserved = reserved

    def __call__(self, vm, hv):
        return hv.get_disk_free(fast=True) - self.reserved > vm.get_disk_size()


class Memory(object):
    """Memory Constraint

    Check if hypervisor has enough memory free to move desired vm there
    """
    def __call__(self, vm, hv):
        return hv.get_memory_free(fast=True) > vm.get_memory()


class EnsureFunctionDistribution(object):
    """Game World / Function Distribution

    Ensure that redundant servers don't reside on the same hypervisor
    """
    def __call__(self, vm, hv):
        for hv_vm in hv.get_vms():
            if hv_vm.hostname == vm.hostname:
                continue
            if hv_vm.get_identifier() == vm.get_identifier():
                return False
        return True


class HypervisorMaxVcpuUsage(object):
    """Hypervisor Max vCPU usage

    Checks the maximum vCPU usage (95 percentile) of the given hypervisor for
    the given time_range and dismisses it as target when it is over the value
    of threshold.
    """
    def __init__(self, threshold):
        self.threshold = threshold

    def __call__(self, vm, hv):
        return hv.get_max_vcpu_usage() < self.threshold
