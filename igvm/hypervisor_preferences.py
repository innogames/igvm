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
import math
import sys
from logging import getLogger
from typing import Union, List, Optional

log = getLogger(__name__)


class HypervisorPreference(object):
    def get_preference(self, vm, hv) -> Union[float, bool]:
        raise NotImplementedError('get_preference is not implemented')


class InsufficientResource(HypervisorPreference):
    """Check a resource of hypervisor would be sufficient"""

    def __init__(
        self,
        hv_attribute: str,
        vm_attribute: str,
        multiplier: int = 1,
        reserved: int = 0,
    ) -> None:
        self.hv_attribute: str = hv_attribute
        self.vm_attribute: str = vm_attribute
        # TODO Use identical units in Serveradmin
        self.multiplier: int = multiplier
        self.reserved: int = reserved

    def __repr__(self) -> str:
        args = repr(self.hv_attribute)
        if self.reserved:
            args += ', reserved=' + repr(self.reserved)

        return '{}({})'.format(type(self).__name__, args)

    def get_preference(self, vm, hv) -> Union[float, bool]:
        # Treat freshly created HVs always passing this check
        if not hv.dataset_obj[self.hv_attribute]:
            return False

        total_size = hv.dataset_obj[self.hv_attribute]
        vms_size = sum(
            vm[self.vm_attribute] * self.multiplier
            for vm in hv.dataset_obj['vms']
        )
        remaining_size = total_size - vms_size - self.reserved

        # does not fit at all
        vm_size = vm.dataset_obj[self.vm_attribute]
        if remaining_size < vm_size:
            return False

        # normalize the resource usage
        return 1 - (vm_size / remaining_size)


class OtherVMs(HypervisorPreference):
    """Count the other VMs on the hypervisor with the same attributes"""

    def __init__(self, attributes=[], values=None) -> None:
        assert values is None or len(attributes) == len(values)
        self.attributes = attributes
        self.values = values

    def __repr__(self) -> str:
        args = ''
        if self.attributes:
            args += repr(self.attributes)
            if self.values:
                args += ', ' + repr(self.values)

        return '{}({})'.format(type(self).__name__, args)

    def get_preference(self, vm, hv) -> Union[float, bool]:
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
                other_vm[attr] == vm.dataset_obj[attr]
                for attr in self.attributes
            ):
                result += 1

        # no similar vms on this hv, that's a good candidate
        if result == 0 or len(hv.dataset_obj['vms']) == 0:
            return 1.

        # normalize the amount of similar vms
        result = 1 - (result / len(hv.dataset_obj['vms']))

        # this is not a hard criteria, but we want to highly discourage
        # similar vms ending up on the same hv, so we apply a harsh factor
        return result * 0.01


class HypervisorAttributeValue(HypervisorPreference):
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

    def get_preference(self, vm, hv) -> Union[float, bool]:
        value = hv.dataset_obj[self.attribute]

        # if there is no value we assume it's a fresh hv
        if value is None:
            return 1.

        # normalize the value. this is only valid for "exhaution values" like
        # cpu_util_pct and iops_avg
        return 1 - (value / 100)


class HypervisorAttributeValueLimit(HypervisorPreference):
    """Compare an attribute value of the hypervisor with the given limit

    We are also handling None in here assuming that it is not exceeding
    the limit.  This is coincidentally matching with the None comparison
    on Python 2.  Although our rationale is that those hypervisors being
    brand new.
    """

    def __init__(self, attribute: str, limit: int) -> None:
        self.attribute: str = attribute
        self.limit: int = limit

    def __repr__(self) -> str:
        args = repr(self.attribute) + ', ' + repr(self.limit)

        return '{}({})'.format(type(self).__name__, args)

    def get_preference(self, vm, hv) -> Union[float, bool]:
        value = hv.dataset_obj[self.attribute]

        # if there is no value we assume it's a fresh hv
        if value is None:
            return 1.

        if value > self.limit:
            return False

        # normalize the value. this is only valid for "exhaution values" like
        # cpu_util_pct and iops_avg
        return 1 - (value / 100)


class HypervisorCpuUsageLimit(HypervisorPreference):
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

    def get_preference(self, vm, hv) -> Union[float, bool]:
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

        # if there is no value we assume it's a fresh hv
        if hv_cpu_util_overall is None:
            return 1.

        # since this is a limiting preference, we don't want any vm end up on
        # the hv that would exceed the cpu threshold
        if hv_cpu_util_overall > hv_cpu_threshold:
            return False

        # normalize the value. this is only valid for "exhaution values" like
        # cpu_util_pct and iops_avg
        return 1 - (hv_cpu_util_overall / hv_cpu_threshold)


class HypervisorEnvironmentValue(HypervisorPreference):
    """Check if the environment of the hypervisor fits with the VM env.

    Make any hypervisor less likely chosen, which would have a different
    environment.
    """

    def __init__(self, hv_env: str):
        self.hv_env = hv_env

    def __repr__(self):
        args = repr(self.hv_env)

        return '{}({})'.format(type(self).__name__, args)

    def get_preference(self, vm, hv) -> Union[float, bool]:
        hypervisor_env = hv.dataset_obj[self.hv_env]
        vm_env = vm.dataset_obj['environment']

        # if the environment is matching, this is our candidate!
        if hypervisor_env == vm_env:
            return 1.

        # unfortunately we don't always have enough hvs to ensure the same
        # environment. therefore we will just highly discourage the hv
        # instead of excluding it
        return 0.01


class OverAllocation(HypervisorPreference):
    """Check for an attribute being over allocated than the current one"""

    def __init__(self, attribute) -> None:
        self.attribute = attribute

    def __repr__(self) -> str:
        args = repr(self.attribute)

        return '{}({})'.format(type(self).__name__, args)

    def get_preference(self, vm, hv) -> Union[float, bool]:
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

        # whether the target hv would be more overbooked than the current one
        rel_overbooking = tgt_ovr_allc / cur_ovr_allc
        if rel_overbooking > 1.:
            return .01

        # normalize the value. this is no hard criteria, except for non-
        # overbookable resources like disk. hence we can allow overbooked cpus.
        # for cpus the avg load is more important, for anything else non-
        # overbookable we will leave the corresponding hard-check for later
        return 1 - rel_overbooking


class PreferenceEvaluator(object):
    def __init__(self, preferences: List[HypervisorPreference]):
        self.preferences = preferences

    def get_preference(self, vm, hv) -> float:
        n_prefs = len(self.preferences)
        matched_prefs = 0
        sum_prefs = 0.

        log.debug('Checking {}..'.format(str(hv)))

        # Checking HV against all preferences
        for p in self.preferences:
            result = float(p.get_preference(vm, hv))

            # We expect normalized values from 0 - 1
            if result < 0. or result > 1.:
                raise ValueError(
                    'preference must be expressed in a 0.0 - 1.0 range, '
                    '{} given'.format(result)
                )

            # Add up the scores
            if result > 0.:
                log.debug('Preference {} matches with score {}'.format(
                    str(p),
                    result,
                ))

                matched_prefs += 1
                sum_prefs += result
            else:
                log.debug('Preference {} does not match'.format(str(p)))

        # Exclude HV if only one criteria has failed
        if matched_prefs < n_prefs:
            log.debug(
                'Hypervisor "{}" excluded, only {}/{} prefs match.'.format(
                    str(hv),
                    matched_prefs,
                    n_prefs,
                )
            )

            return 0.

        # Calculate the overall preference score of the target hv
        total = (sum_prefs / (n_prefs - matched_prefs + 1)) / n_prefs

        log.debug('Matching {}/{} prefs with a total score of {}'.format(
            matched_prefs,
            n_prefs,
            total,
        ))

        log.info('Hypervisor "{}" selected with a {} score.'.format(
            str(hv),
            total,
        ))

        return total


class PreferredHypervisor(object):
    def __init__(self, hv, score: float):
        self._hv = hv
        self._score = score

    def __lt__(self, other):
        return self._score < other.score()

    def __eq__(self, other):
        return self._score == other.score()

    def hv(self):
        return self._hv

    def score(self) -> float:
        return self._score


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

    evaluator = PreferenceEvaluator(preferences)
    preferred_hvs = []

    # Collect all hvs that are possible to migrate to
    for hv in hypervisors:
        total = evaluator.get_preference(vm, hv)
        if total > 0.:
            preferred_hvs.append(PreferredHypervisor(hv, total))

    # Sort reversed as we want hvs with higher scores first
    preferred_hvs = sorted(preferred_hvs, reverse=True)

    # Yield sorted hvs
    for preferred_hv in preferred_hvs:
        yield preferred_hv.hv()
