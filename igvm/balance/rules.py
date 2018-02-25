"""igvm - Balancing Rules

Copyright (c) 2018, InnoGames GmbH
"""


class Rule(object):
    """Base Rule Class

    This class is the base rule class from which all concrete rules should
    inherit to fulfil the base contract of how to access it. Within all classes
    the inherit from Constraint you have access to the following standard
    attributes as well as all key, values as attributes you attached in
    configuration.

    :param: name: speaking name of constraint from config.json
    :param: weight: weight by specifying a factor such as e.g. 0.25 or 2

    :return:
    """

    def __init__(self, *args, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)

    def score(self, vm, hv):
        """Get score the current selection

        Must return a float as a ranking value for the hypervisor. Where more
        is better and less is not so good. If in doubt you can return a
        percentage value. It is safe to mix up certain rules without fucking up
        weight of multiple rules because each rule ranking is evaluate and best
        hypervisor gets the maximum amount of points for this round see down
        under example.

        :param: vm: igvm.balance.models.VM object
        :param: hv: igvm.balance.models.Hypervisor object

        :return: float

            aw-hv-100 => 97.0 => 2 points
            aw-hv-102 => 98.0 => 1 point

        """

        raise NotImplementedError()


class HypervisorMaxCpuUsage(Rule):
    """Hypervisor maximum CPU usage of the last 24h with 95 percentile score

    Evaluates the maximum CPU usage of the last 24h and returns a score for it.

    :return: score
    """

    def __init__(self, *args, **kwargs):
        super(HypervisorMaxCpuUsage, self).__init__(*args, **kwargs)

    def score(self, vm, hv):
        return 100.0 - (hv.get_max_cpu_usage() or 100.0)


class CpuOverAllocation(Rule):
    """CPU Over Allocation

    Rate the CPU over allocation of the target hypervisor and if it better or
    worse than the current one.

    :return:
    """

    def __init__(self, *args, **kwargs):
        super(CpuOverAllocation, self).__init__(*args, **kwargs)

    def score(self, vm, hv):
        # New VM has no xen_host attribute yet.
        if not vm['xen_host']:
            return 100.0

        cur_hv_cpus = sum(v['num_cpu'] for v in vm.get_hypervisor().get_vms())
        cur_hv_rl_cpus = vm.get_hypervisor()['num_cpu']
        cur_ovr_allc = float(cur_hv_cpus) / float(cur_hv_rl_cpus)

        tgt_hv_cpus = vm['num_cpu'] + sum(v['num_cpu'] for v in hv.get_vms())
        tgt_hv_rl_cpus = hv['num_cpu']
        tgt_ovr_allc = float(tgt_hv_cpus) / float(tgt_hv_rl_cpus)

        if tgt_ovr_allc < cur_ovr_allc:
            return 100.0
        if tgt_ovr_allc == cur_ovr_allc:
            return 50.0
        return 0.0
