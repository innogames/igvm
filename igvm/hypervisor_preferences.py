"""igvm - Hypervisor Selection Preferences

This module contains preferences to select hypervisors.  Preferences return
a value of any comparable datatype.  Only the return values of the same
preference is compared with each other.  Smaller values mark hypervisors
as more preferred.  Keep in mind that for booleans false is less than true.
See sorted_hypervisors() function below for the details of sorting.

Copyright (c) 2018 InnoGames GmbH
"""
# This module contains the preferences as simple classes.  We try to keep
# them reusable, even though most of them are not reused.  Some of the classes
# are so simple that they could as well just be a function, but kept
# as classes to have a consistent style.

from logging import getLogger

from igvm.utils import LazyCompare

log = getLogger(__name__)


class InsufficientResource(object):
    """Check a resource of hypervisor would be sufficient"""
    def __init__(self, hv_attribute, vm_attribute, multiplier=1, reserved=0):
        self.hv_attribute = hv_attribute
        self.vm_attribute = vm_attribute
        self.multiplier = multiplier  # TODO Use identical units in Serveradmin
        self.reserved = reserved

    def __repr__(self):
        args = repr(self.hv_attribute)
        if self.reserved:
            args += ', reserved=' + repr(self.reserved)

        return '{}({})'.format(type(self).__name__, args)

    def __call__(self, vm, hv):
        # Treat freshly created HVs always passing this check
        if not hv.dataset_obj[self.hv_attribute]:
            return False

        total_size = hv.dataset_obj[self.hv_attribute]
        vms_size = sum(
            vm[self.vm_attribute] * self.multiplier
            for vm in hv.dataset_obj['vms']
        )
        remaining_size = total_size - vms_size - self.reserved

        return remaining_size < vm.dataset_obj[self.vm_attribute]


class OtherVMs(object):
    """Count the other VMs on the hypervisor with the same attributes"""
    def __init__(self, attributes=[], values=None):
        assert values is None or len(attributes) == len(values)
        self.attributes = attributes
        self.values = values

    def __repr__(self):
        args = ''
        if self.attributes:
            args += repr(self.attributes)
            if self.values:
                args += ', ' + repr(self.values)

        return '{}({})'.format(type(self).__name__, args)

    def __call__(self, vm, hv):
        result = 0
        for other_vm in hv.dataset_obj['vms']:
            if other_vm['hostname'] == vm.dataset_obj['hostname']:
                continue
            if self.values and not all(
                vm.dataset_obj[a] == v
                for a, v in zip(self.attributes, self.values)
            ):
                continue
            if all(
                other_vm[a] == vm.dataset_obj[a]
                for a in self.attributes
            ):
                result += 1

        return result


class HypervisorAttributeValue(object):
    """Return an attribute value of the hypervisor

    We are also handling None in here assuming that it is less than
    anything else.  This is coincidentally matching with the None comparison
    on Python 2.  Although our rationale is that those hypervisors being
    brand new.
    """
    def __init__(self, attribute):
        self.attribute = attribute

    def __repr__(self):
        args = repr(self.attribute)

        return '{}({})'.format(type(self).__name__, args)

    def __call__(self, vm, hv):
        value = hv.dataset_obj[self.attribute]

        return value is not None, value


class HypervisorAttributeValueLimit(object):
    """Compare an attribute value of the hypervisor with the given limit

    We are also handling None in here assuming that it is not exceeding
    the limit.  This is coincidentally matching with the None comparison
    on Python 2.  Although our rationale is that those hypervisors being
    brand new.
    """
    def __init__(self, attribute, limit):
        self.attribute = attribute
        self.limit = limit

    def __repr__(self):
        args = repr(self.attribute) + ', ' + repr(self.limit)

        return '{}({})'.format(type(self).__name__, args)

    def __call__(self, vm, hv):
        value = hv.dataset_obj[self.attribute]

        return value is not None and value > self.limit


class HypervisorCpuUsageLimit():
    """Check for CPU usage of the hypervisor incl. the predicted CPU usage
    of the VM to be migrated.

    Make any hypervisor less likely chosen, which would be above its threshold.
    """
    def __init__(self, hardware_model: str, hv_cpu_thresholds: dict):
        self.hardware_model = hardware_model
        self.hv_cpu_thresholds = hv_cpu_thresholds

    def __repr__(self):
        args = repr(self.hardware_model) + ', ' + repr(self.hv_cpu_thresholds)

        return '{}({})'.format(type(self).__name__, args)

    def __call__(self, vm, hv) -> bool:
        # New VM has no hypervisor attribute yet.
        if not vm.hypervisor:
            return False

        hv_model = hv.dataset_obj[self.hardware_model]

        # Bail out if hardware_model is not in HYPERVISOR_CPU_THRESHOLDS list
        if hv_model not in self.hv_cpu_thresholds:
            log.warning(
                'Missing setting for "{}" in HYPERVISOR_CPU_THRESHOLDS'.format(
                    hv_model))
            return False

        hv_cpu_threshold = float(self.hv_cpu_thresholds[hv_model])
        hv_cpu_util_overall = hv.estimate_cpu_usage(vm)

        return (
            hv_cpu_util_overall is not None and
            hv_cpu_util_overall > hv_cpu_threshold
        )


class OverAllocation(object):
    """Check for an attribute being over allocated than the current one"""
    def __init__(self, attribute):
        self.attribute = attribute

    def __repr__(self):
        args = repr(self.attribute)

        return '{}({})'.format(type(self).__name__, args)

    def __call__(self, vm, hv):
        # New VM has no hypervisor attribute yet.
        if not vm.hypervisor:
            return False

        cur_hv_cpus = sum(
            v[self.attribute] for v in vm.hypervisor.dataset_obj['vms']
        )
        cur_hv_rl_cpus = vm.hypervisor.dataset_obj[self.attribute]
        cur_ovr_allc = float(cur_hv_cpus) / float(cur_hv_rl_cpus)

        tgt_hv_cpus = vm.dataset_obj[self.attribute] + sum(
            v[self.attribute] for v in hv.dataset_obj['vms']
        )
        tgt_hv_rl_cpus = hv.dataset_obj[self.attribute]
        tgt_ovr_allc = float(tgt_hv_cpus) / float(tgt_hv_rl_cpus)

        return tgt_ovr_allc > cur_ovr_allc


class HashDifference(object):
    """Return some arbitrary number to have stable ordering"""
    def __repr__(self):
        return '{}()'.format(type(self).__name__)

    def __call__(self, vm, hv):
        return hash(hv.fqdn) - hash(vm.fqdn)


def sorted_hypervisors(preferences, vm, hypervisors):
    """Sort the hypervisor by their preference

    The most preferred ones will be yielded first.  The caller may then verify
    and use the hypervisors.  For sorting, we simply put the results
    of the preferences to any array for every hypervisor, and let Python
    sort the arrays.  Unlike semantically-low-level programming languages
    like C, Python can compare arrays just fine.  It would recursively compare
    the elements of the arrays with each other, and stop when they differ.

    As we know that most of the time it wouldn't be necessary to compare
    all elements of the arrays with each other, we don't need to prepare
    the results of the preferences for every hypervisor.  We use LazyCompare
    class to let them be prepared lazily.  When Python needs to compare
    and element of the array first time, the preference is going to be
    executed for that hypervisor by LazyCompare.

    The LazyCompare optimization has one other benefit of providing some
    visibility about the selection.  After the sorting is done, we can check
    which preferences are actually executed for the selected hypervisor,
    and log which preference caused this hypervisor to be sorted after
    the previous or before the next one.
    """
    log.debug('Sorting hypervisors by preference...')

    # Use decorate-sort-undecorate pattern to log details about sorting
    for comparables, hypervisor in sorted(
        ([LazyCompare(p, vm, h) for p in preferences], h)
        for h in hypervisors
    ):
        for executed, comparable in enumerate(comparables):
            if not comparable.executed:
                break
        else:
            executed = len(comparables)
        log.info(
            'Hypervisor "{}" selected using {} preferences.'
            .format(hypervisor, executed)
        )

        yield hypervisor
