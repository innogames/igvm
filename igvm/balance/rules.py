"""igvm - Balancing Rules

This module contains rules to select hypervisors.  Rules are to make some
hypervisors more preferred.  They must return a float as a ranking value for
the hypervisor.  Where more is better.  If in doubt, you can return
a percentage value.  It is safe to mix up certain rules without fucking up
weight of multiple rules because each rule ranking is evaluate and best
hypervisor gets the maximum amount of points for this round see down under
example.

Copyright (c) 2018, InnoGames GmbH
"""


class HypervisorMaxCpuUsage(object):
    """Hypervisor maximum CPU usage of the last 24h with 95 percentile score

    Evaluates the maximum CPU usage of the last 24h and returns a score for it.
    """
    def __call__(self, vm, hv):
        return 100.0 - (hv.get_max_cpu_usage() or 100.0)


class CpuOverAllocation(object):
    """CPU Over Allocation

    Rate the CPU over allocation of the target hypervisor and if it better or
    worse than the current one.
    """
    def __call__(self, vm, hv):
        # New VM has no xen_host attribute yet.
        if not vm['xen_host']:
            return 100.0

        cur_hv_cpus = sum(v['num_cpu'] for v in vm.hypervisor.get_vms())
        cur_hv_rl_cpus = vm.hypervisor['num_cpu']
        cur_ovr_allc = float(cur_hv_cpus) / float(cur_hv_rl_cpus)

        tgt_hv_cpus = vm['num_cpu'] + sum(v['num_cpu'] for v in hv.get_vms())
        tgt_hv_rl_cpus = hv['num_cpu']
        tgt_ovr_allc = float(tgt_hv_cpus) / float(tgt_hv_rl_cpus)

        if tgt_ovr_allc < cur_ovr_allc:
            return 100.0
        if tgt_ovr_allc == cur_ovr_allc:
            return 50.0
        return 0.0
