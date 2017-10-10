from threading import Thread

from igvm.balance.models import GameMarket


class Rule(Thread):
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
        Thread.__init__(self)
        self.started = False

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

    def run(self):
        """Wrapper for theading support"""

        self.started = True
        self.score = self.score(self.vm, self.hv)


class HypervisorMaxCpuUsage(Rule):
    """Hypervisor maximum CPU usage of the last 24h with 95 percentile score

    Evaluates the maximum CPU usage of the last 24h and returns a score for it.

    :return: score
    """

    def __init__(self, *args, **kwargs):
        super(HypervisorMaxCpuUsage, self).__init__(*args, **kwargs)

    def score(self, vm, hv):
        max_usage = hv.get_max_cpu_usage()

        # Should not be possible but lets better be sure since graphite
        # sometimes returns weird data.
        if max_usage < 0 or max_usage > 100:
            return 0.0

        score = 100.0 - max_usage

        return score


class GameMarketDistribution(Rule):
    """Game Market Distribution

    Rate the distribution of the game market to ensure to not put too much VMs
    if a game market on the same hypervisor.

    :return:
    """

    def __init__(self, *args, **kwargs):
        super(GameMarketDistribution, self).__init__(*args, **kwargs)

    def score(self, vm, hv):
        gm = GameMarket(vm.get_game(), vm.get_market())

        same_hv = 0.0
        single_vm_weight = 100.0 / float(len(gm.get_vms()))
        for cur_vm in gm.get_vms():
            if cur_vm.get_hypervisor() == hv:
                same_hv += 1.0

        return 100.0 - (same_hv * single_vm_weight)


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
        if not vm.get_serveradmin_data()['xen_host']:
            return 100.0

        cur_hv_cpus = 0
        for cur_vm in vm.get_hypervisor().get_vms():
            cur_hv_cpus += cur_vm.get_serveradmin_data()['num_cpu']

        tgt_hv_cpus = vm.get_serveradmin_data()['num_cpu']
        for cur_vm in hv.get_vms():
            tgt_hv_cpus += cur_vm.get_serveradmin_data()['num_cpu']

        cur_hv_rl_cpus = vm.get_hypervisor().get_serveradmin_data()['num_cpu']
        cur_ovr_allc = float(cur_hv_cpus) / float(cur_hv_rl_cpus) * 100.0

        tgt_hv_rl_cpus = hv.get_serveradmin_data()['num_cpu']
        tgt_ovr_allc = float(tgt_hv_cpus) / float(tgt_hv_rl_cpus) * 100.0

        if tgt_ovr_allc < cur_ovr_allc:
            score = 100.0
        elif tgt_ovr_allc == cur_ovr_allc:
            score = 50.0
        else:
            score = 0.0

        return score
