"""igvm - Hypervisor Preferences

This module contains preferences to select hypervisors.  Preferences
return a value of any comparable datatype.  Only the return values of
the same preference is compared with each other.  Smaller values mark
hypervisors as more preferred.  Keep in mind that for booleans false
is less than true.

Copyright (c) 2018, InnoGames GmbH
"""


class InsufficientResource(object):
    """Check a resource of hypervisor would be sufficient"""
    def __init__(self, attribute, reserved=0):
        self.attribute = attribute
        self.reserved = reserved

    def __repr__(self):
        args = repr(self.attribute)
        if self.reserved:
            args += ', reserved=' + repr(self.reserved)

        return '{}({})'.format(type(self).__name__, args)

    def __call__(self, vm, hv):
        total_size = hv.dataset_obj[self.attribute]
        vms_size = sum(v[self.attribute] for v in hv.dataset_obj['vms'])
        remaining_size = total_size - vms_size - self.reserved

        return remaining_size < vm.dataset_obj[self.attribute]


class OtherVMsWithSameAttributes(object):
    """Count the other VMs on the hypervisor with the same attributes"""
    def __init__(self, attributes, values=None):
        assert values is None or len(attributes) == len(values)
        self.attributes = attributes
        self.values = values

    def __repr__(self):
        args = repr(self.attributes)
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
                result = 1

        return result


class HypervisorAttributeValue(object):
    """Return inverse of an attribute value of the hypervisor"""
    def __init__(self, attribute):
        self.attribute = attribute

    def __repr__(self):
        args = repr(self.attribute)

        return '{}({})'.format(type(self).__name__, args)

    def __call__(self, vm, hv):
        return hv.dataset_obj[self.attribute]


class HypervisorAttributeValueLimit(object):
    """Compare an attribute value of the hypervisor with the given limit"""
    def __init__(self, attribute, limit):
        self.attribute = attribute
        self.limit = limit

    def __repr__(self):
        args = repr(self.attribute) + ', ' + repr(self.limit)

        return '{}({})'.format(type(self).__name__, args)

    def __call__(self, vm, hv):
        return hv.dataset_obj[self.attribute] > self.limit


class OverAllocation(object):
    """Check for an attribute being over allocated than the current one"""
    def __init__(self, attribute):
        self.attribute = attribute

    def __repr__(self):
        args = repr(self.attribute)

        return '{}({})'.format(type(self).__name__, args)

    def __call__(self, vm, hv):
        # New VM has no xen_host attribute yet.
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
