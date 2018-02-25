"""igvm - Hypervisor Preferences

This module contains preferences to select hypervisors.  Preferences
return a value of any comparable datatype.  Only the return values of
the same preference is compared with each other.  Greater values mark
hypervisors as more preferred.  Keep in mind that for booleans true
is greater than false.

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
        total_size = hv.dataset_obj['disk_size_gib']
        # We assume 10 GiB for root partition and 16 for swap.
        host_size = 16 + 10
        vms_size = sum(v['disk_size_gib'] for v in hv.dataset_obj['vms'])
        remaining_size = total_size - vms_size - host_size - self.reserved

        return remaining_size > vm.dataset_obj['disk_size_gib']


class Memory(object):
    """Memory Constraint

    Check if enough memory is free on target hypervisor.  Memory is estimated
    using disk space of the VMs.
    """
    def __call__(self, vm, hv):
        vms_memory = sum(v['memory'] for v in hv.dataset_obj['vms'])

        return hv.dataset_obj['memory'] - vms_memory > vm.dataset_obj['memory']


class EnsureFunctionDistribution(object):
    """Game World / Function Distribution

    Ensure that redundant servers don't reside on the same hypervisor
    """
    def __call__(self, vm, hv):
        for other_vm in hv.dataset_obj['vms']:
            if other_vm['hostname'] == vm.dataset_obj['hostname']:
                continue
            if (
                self.get_identifier(other_vm) !=
                self.get_identifier(vm.dataset_obj)
            ):
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
        return hv.dataset_obj['cpu_util_vm_pct'] < self.threshold


class HypervisorMaxCpuUsage(object):
    """Hypervisor maximum CPU usage of the last 24h with 95 percentile score

    Evaluates the maximum CPU usage of the last 24h and returns a score for it.
    """
    def __call__(self, vm, hv):
        return 100.0 - (hv.dataset_obj['cpu_util_pct'] or 100.0)


class CpuOverAllocation(object):
    """CPU Over Allocation

    Rate the CPU over allocation of the target hypervisor by it being better
    or worse than the current one.
    """
    def __call__(self, vm, hv):
        # New VM has no xen_host attribute yet.
        if not vm.hypervisor:
            return 100.0

        cur_hv_cpus = sum(
            v['num_cpu'] for v in vm.hypervisor.dataset_obj['vms']
        )
        cur_hv_rl_cpus = vm.hypervisor.dataset_obj['num_cpu']
        cur_ovr_allc = float(cur_hv_cpus) / float(cur_hv_rl_cpus)

        tgt_hv_cpus = vm.dataset_obj['num_cpu'] + sum(
            v['num_cpu'] for v in hv.dataset_obj['vms']
        )
        tgt_hv_rl_cpus = hv.dataset_obj['num_cpu']
        tgt_ovr_allc = float(tgt_hv_cpus) / float(tgt_hv_rl_cpus)

        if tgt_ovr_allc < cur_ovr_allc:
            return 100.0
        if tgt_ovr_allc == cur_ovr_allc:
            return 50.0
        return 0.0
