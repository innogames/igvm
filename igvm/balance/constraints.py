"""igvm - Balancing Constraints

This module contains constraints to select hypervisors.

Copyright (c) 2018, InnoGames GmbH
"""


class DiskSpace(object):
    """Disk Space Constraint

    Check if enough disk space is free on target hypervisor.  Disk space is
    estimated using disk space of the VMs.
    """
    def __init__(self, reserved):
        self.reserved = reserved

    def __call__(self, vm, hv):
        total_size = hv['disk_size_gib']
        # We assume 10 GiB for root partition and 16 for swap.
        host_size = 16 + 10
        vms_size = sum(v['disk_size_gib'] for v in hv['vms'])
        remaining_size = total_size - vms_size - host_size - self.reserved

        return remaining_size > vm['disk_size_gib']


class Memory(object):
    """Memory Constraint

    Check if enough memory is free on target hypervisor.  Memory is estimated
    using disk space of the VMs.
    """
    def __call__(self, vm, hv):
        vms_memory = sum(v['memory'] for v in hv['vms'])

        return hv['memory'] - vms_memory > vm['memory']


class EnsureFunctionDistribution(object):
    """Game World / Function Distribution

    Ensure that redundant servers don't reside on the same hypervisor
    """
    def __call__(self, vm, hv):
        for other_vm in hv['vms']:
            if other_vm['hostname'] == vm['hostname']:
                continue
            if self.get_identifier(other_vm) != self.get_identifier(vm):
                return False
        return True

    def get_identifier(self, dataset_obj):
        if dataset_obj['game_market'] and dataset_obj['game_world']:
            identifier = '{}-{}-{}'.format(
                dataset_obj['project'],
                dataset_obj['game_market'],
                dataset_obj['game_world'],
            )

            if dataset_obj['game_type']:
                identifier += (
                    '-' + dataset_obj['game_type']
                )

            return identifier
        else:
            return '{}-{}-{}'.format(
                dataset_obj['project'],
                dataset_obj['function'],
                dataset_obj['environment']
            )


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
