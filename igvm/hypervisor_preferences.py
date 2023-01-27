"""igvm - Hypervisor Selection Preferences

This module contains preference classes to select Hypervisors. Preferences
return either a score of range 0.0 - 1.0 inclusive, or a boolean value, which
is only there for convenience and converts to float.
A higher score means that a Hypervisor should be favored, while a low one
discourages a HV. If False or 0.0 is given, that means a HV does not fulfill
the requirements at all and is excluded.

Copyright (c) 2021 InnoGames GmbH
"""
import abc
from logging import getLogger
from typing import Union, List

log = getLogger(__name__)


class HypervisorPreference(abc.ABC):
    """The base class for all HV preferences."""

    @abc.abstractmethod
    def get_score(self, vm, hv) -> Union[float, bool]:
        """Calculates a preference value to indicate how good a HV fits.

        @param vm: The VM object to check against the HV.
        @param hv: The HV object to check against the VM.
        @return: A float or a bool. The bool is just for convenience and will
                 be converted to a float. A value of 0.0 means the HV does not
                 fulfill the requirement at all. But a value of 1.0 means it is
                 a very good candidate for this particular preference.
        @rtype: Union[float, bool]
        """


class InsufficientResource(HypervisorPreference):
    """Check whether a resource of a hypervisor would be sufficient."""

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

    def get_score(self, vm, hv) -> Union[float, bool]:
        # Treat freshly created HVs always passing this check
        if not hv.dataset_obj[self.hv_attribute]:
            # Because new installed Hypervisors don't have values 
            # (e.g. for load) - they are fresh meat ...
            return True

        # Calculate the remaining "size" of the resource
        total_size = hv.dataset_obj[self.hv_attribute] * self.multiplier
        vms_size = sum(
            vm[self.vm_attribute]
            for vm in hv.dataset_obj['vms']
            if vm['state'] != 'retired'
        )
        remaining_size = total_size - vms_size - self.reserved

        # Does not fit at all
        vm_size = vm.dataset_obj[self.vm_attribute]
        if remaining_size < vm_size:
            return False

        # Normalize the expected resource consumption
        return float(1 - (vm_size / remaining_size))


class OtherVMs(HypervisorPreference):
    """Count the other VMs on the hypervisor with the same attributes."""

    def __init__(self, attributes: list, values: list = None) -> None:
        assert values is None or len(attributes) == len(values)

        self.attributes: list = attributes
        self.values: list = values

    def __repr__(self) -> str:
        args = ''
        if self.attributes:
            args += repr(self.attributes)
            if self.values:
                args += ', ' + repr(self.values)

        return '{}({})'.format(type(self).__name__, args)

    def get_score(self, vm, hv) -> Union[float, bool]:
        # If there are no VMs at all that makes a perfect match.
        if len(hv.dataset_obj['vms']) == 0:
            return 1.

        # Count similar VMs on the HV.
        n_similar = 0
        for other_vm in hv.dataset_obj['vms']:
            # Exclude ourselves.
            if other_vm['hostname'] == vm.dataset_obj['hostname']:
                continue

            # Check for specifically given attribute values.
            if self.values and not all(
                vm.dataset_obj[attr] == val
                for attr, val in zip(self.attributes, self.values)
            ):
                continue

            # Check for same attribute values.
            if not all(
                other_vm[attr] == vm.dataset_obj[attr]
                for attr in self.attributes
            ):
                continue

            # There is indeed a similar VM on the HV.
            n_similar += 1

        # No similar vms on this hv, that's a good candidate.
        if n_similar == 0:
            return 1.

        # Normalize the amount of similar vms. This is kind of difficult to do,
        # because we have no idea what is the maximum amount of VMs on a HV.
        # It solely depends on how loaded a HV is and how many resources are
        # left. Hence we can only give some "virtual" score here. We are
        # setting the maximum to an imaginary 100 VMs.
        # TODO: come up with something better. idea: take all HVs into account
        result = 1 - (n_similar / 100)

        # This is not a hard criteria, but we want to highly discourage
        # similar vms ending up on the same hv, so we apply a harsh factor.
        if result == 0.:
            return 0.01

        return result * 0.01


class HypervisorAttributeValue(HypervisorPreference):
    """Return a score based on a percentage-based attribute value."""

    def __init__(self, attribute: str) -> None:
        self.attribute: str = attribute

    def __repr__(self) -> str:
        args = repr(self.attribute)

        return '{}({})'.format(type(self).__name__, args)

    def get_score(self, vm, hv) -> Union[float, bool]:
        value = hv.dataset_obj[self.attribute]

        # If there is no value we assume it's a fresh HV.
        if value is None:
            return 1.

        # Normalize the value. This is only valid for percentage values like
        # cpu_util_pct and iops_avg. Arbitrary numbers are somewhat difficult
        # to put into context.
        return float(1 - (value / 100))


class HypervisorAttributeValueLimit(HypervisorPreference):
    """Score a percentage-based attribute value against a given limit."""

    def __init__(self, attribute: str, limit: int) -> None:
        self.attribute: str = attribute
        self.limit: int = limit

    def __repr__(self) -> str:
        args = repr(self.attribute) + ', ' + repr(self.limit)

        return '{}({})'.format(type(self).__name__, args)

    def get_score(self, vm, hv) -> Union[float, bool]:
        value = hv.dataset_obj[self.attribute]

        # If there is no value we assume it's a fresh HV.
        if value is None:
            return 1.

        # When the actual value is above the limit, we strike out that HV.
        if value > self.limit:
            log.warning(
                f'Hypervisor "{str(hv)}" skipped because {self.attribute} '
                'attribute is higher '
                f'than expected ({value} > {self.limit}).',
            )
            return False

        # Normalize the value. This is only valid for percentage values like
        # cpu_util_pct and iops_avg. Arbitrary numbers are somewhat difficult
        # to put into context.
        return float(1 - (value / 100))


class HypervisorCpuUsageLimit(HypervisorPreference):
    """Check for CPU usage of the hypervisor incl. the predicted CPU usage
    of the VM to be migrated.

    Make any hypervisor less likely chosen, which would be above its threshold.
    """

    def __init__(self, hardware_model: str, hv_cpu_thresholds: dict) -> None:
        self.hardware_model: str = hardware_model
        self.hv_cpu_thresholds: dict = hv_cpu_thresholds

    def __repr__(self) -> str:
        args = repr(self.hardware_model) + ', ' + repr(self.hv_cpu_thresholds)

        return '{}({})'.format(type(self).__name__, args)

    def get_score(self, vm, hv) -> Union[float, bool]:
        # New VM has no hypervisor attribute, yet, so we cannot calculate a
        # score here. We will just allow all HVs to take that VM for now.
        if not vm.hypervisor:
            return True

        hv_model = hv.dataset_obj[self.hardware_model]

        # Bail out if hardware_model is not in HYPERVISOR_CPU_THRESHOLDS list.
        if hv_model not in self.hv_cpu_thresholds:
            log.error(
                'Missing setting for "{}" in HYPERVISOR_CPU_THRESHOLDS'.format(
                    hv_model,
                ),
            )
            return False

        hv_cpu_threshold = float(self.hv_cpu_thresholds[hv_model])
        hv_cpu_util_overall = hv.estimate_cpu_usage(vm)

        # If there is no value we assume it's a fresh hv.
        if hv_cpu_util_overall is None:
            return True

        # Since this is a limiting preference, we don't want any vm end up on
        # the hv that would exceed the cpu threshold
        if hv_cpu_util_overall > hv_cpu_threshold:
            return False

        # Normalize the value. This is only valid for percentage values like
        # cpu_util_pct and iops_avg. Arbitrary numbers are somewhat difficult
        # to put into context.
        return float(1 - (hv_cpu_util_overall / hv_cpu_threshold))


class HypervisorEnvironmentValue(HypervisorPreference):
    """Check if the environment of the hypervisor fits with the VM env.

    Make any hypervisor less likely chosen, which would have a different
    environment.
    """

    def __init__(self, hv_env: str) -> None:
        self.hv_env: str = hv_env

    def __repr__(self) -> str:
        args = repr(self.hv_env)

        return '{}({})'.format(type(self).__name__, args)

    def get_score(self, vm, hv) -> Union[float, bool]:
        hypervisor_env = hv.dataset_obj[self.hv_env]
        vm_env = vm.dataset_obj['environment']

        # If the environment is matching, this is our candidate!
        if hypervisor_env == vm_env:
            return 1.

        # Unfortunately we don't always have enough HVs to ensure the same
        # environment. Therefore we will just highly discourage the hv
        # instead of excluding it.
        return 0.01


class OverAllocation(HypervisorPreference):
    """Check for an attribute being over-allocated than the current one."""

    def __init__(self, attribute) -> None:
        self.attribute = attribute

    def __repr__(self) -> str:
        args = repr(self.attribute)

        return '{}({})'.format(type(self).__name__, args)

    def get_score(self, vm, hv) -> Union[float, bool]:
        # New VM has no hypervisor attribute, yet, so we cannot calculate a
        # score here. We will just allow all HVs to take that VM for now.
        if not vm.hypervisor:
            return True

        # Calculate the current HVs overbooking "level".
        cur_hv_cpus = sum(
            v[self.attribute] for v in vm.hypervisor.dataset_obj['vms']
        )
        cur_hv_rl_cpus = vm.hypervisor.dataset_obj[self.attribute]
        cur_ovr_allc = float(cur_hv_cpus) / float(cur_hv_rl_cpus)

        # Calculate by how much we would overbook the target HV.
        tgt_hv_cpus = vm.dataset_obj[self.attribute] + sum(
            v[self.attribute] for v in hv.dataset_obj['vms']
        )
        tgt_hv_rl_cpus = hv.dataset_obj[self.attribute]
        tgt_ovr_allc = float(tgt_hv_cpus) / float(tgt_hv_rl_cpus)

        # Whether the target hv would be more overbooked than the current one.
        rel_overbooking = tgt_ovr_allc / cur_ovr_allc
        if rel_overbooking > 1.:
            return .01

        # Normalize the value. We usually don't overbook any resources because
        # they are limited (memory, disk). However we do overbook CPUs. This
        # is a "soft" preference. The hard checks are done at a later point by
        # communicating directly with libvirt. For this reason we will treat
        # everything as overbookable here, but still discourage it.
        # For treating this as "hard" criteria, use InsufficientResource.
        return float(1 - rel_overbooking)


class PreferenceEvaluator:
    """Evaluates all preferences for a given VM and HV and calculates the total
    score based on which the most preferred HVs can be picked.
    """
    def __init__(
        self,
        preferences: List[HypervisorPreference],
        soft: bool = False,
    ) -> None:
        self.preferences = preferences
        self.soft = soft

    def get_total_score(self, vm, hv) -> float:
        """Calculates the total score for a given VM and HV pair."""
        n_prefs = len(self.preferences)
        matched_prefs = 0
        sum_prefs = 0.

        log.debug('Checking {}..'.format(str(hv)))

        # Checking HV against all preferences.
        for pref in self.preferences:
            result = float(pref.get_score(vm, hv))

            # We expect normalized values from 0 - 1.
            if result < 0. or result > 1.:
                raise ValueError(
                    'Preference "{}" for Hypervisor "{}" must be expressed '
                    'in a 0.0 - 1.0 range, {} given.'.format(
                        str(pref),
                        str(hv),
                        result,
                    )
                )

            # Add up the individual preference scores.
            if result > 0.:
                log.debug('Preference "{}" matches with score {:.4f}.'.format(
                    str(pref),
                    result,
                ))

                matched_prefs += 1
                sum_prefs += result
            elif not self.soft:
                log.debug(
                    'Hypervisor "{}" is skipped because preference "{}" does '
                    'not match.'.format(str(hv), str(pref)),
                )
            else:
                log.debug('Preference "{}" does not match.'.format(str(pref)))

        # If run in "strict" mode the HV is immediately excluded if any of the
        # preferences fails. If run in "soft" mode, they are not excluded but
        # ranked much lower accordingly.
        if not self.soft and matched_prefs < n_prefs:
            log.debug(
                'Hypervisor "{}" excluded, only {}/{} prefs match.'.format(
                    str(hv),
                    matched_prefs,
                    n_prefs,
                )
            )

            return 0.
        elif matched_prefs < n_prefs:
            log.warning(
                'Hypervisor "{}" kept although it would normally be '
                'skipped because {} preferences do not match.'.format(
                    str(hv),
                    n_prefs - matched_prefs,
                ),
            )

        # Calculate the overall preference score of the target HV. If run in
        # "soft" mode, the total score will be adjusted to rank it lower.
        # Examples:
        #   n_prefs = 10
        #   matched_prefs = 10
        #   sum_prefs = 10 (10 matching preferences with score 1.0 each)
        #   total = (10/(10-10+1))/10 = 1
        #
        #   matched_prefs = 9
        #   sum_prefs = 9 (9 matching preferences with score 1.0 each)
        #   total = (9/(10-9+1))/10 = 0.45
        #
        #   matched_prefs = 1
        #   sum_prefs = 1 (1 matching preference with score 1.0)
        #   total = (1/(10-1+1))/10 = 0.01
        total = (sum_prefs / (n_prefs - matched_prefs + 1)) / n_prefs

        log.debug('Matching {}/{} prefs with a total score of {:.4f}.'.format(
            matched_prefs,
            n_prefs,
            total,
        ))

        log.info('Hypervisor "{}" selected with a {:.4f} score.'.format(
            str(hv),
            total,
        ))

        return total


class PreferredHypervisor:
    """Sortable container holding a HV object along with it's score."""

    def __init__(self, hv, score: float) -> None:
        self._hv = hv
        self._score = score

    def __lt__(self, other) -> bool:
        return self._score < other.score()

    def __eq__(self, other) -> bool:
        return self._score == other.score()

    def hv(self):
        return self._hv

    def score(self) -> float:
        return self._score


def sort_by_preference(
    vm,
    preferences,
    hypervisors,
    soft: bool = False,
) -> list:
    """Sort the hypervisors by their preference scores.

    The most preferred ones will be first. The caller may then verify and use
    the Hypervisors. Hypervisors with higher scores will be favored compared to
    others, though Hypervisors with a zero score will be excluded altogether.
    """
    log.debug('Sorting hypervisors by preference score..')

    evaluator = PreferenceEvaluator(preferences, soft=soft)
    preferred_hvs = []

    # Collect all HVs that are possible to migrate to.
    for hv in hypervisors:
        score = evaluator.get_total_score(vm, hv)
        if score > 0.:
            preferred_hvs.append(PreferredHypervisor(hv, score))

    # Sort reversed as we want hvs with higher scores first.
    preferred_hvs = sorted(preferred_hvs, reverse=True)

    # Return sorted HVs.
    return [preferred_hv.hv() for preferred_hv in preferred_hvs]
