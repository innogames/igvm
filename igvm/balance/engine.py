"""igvm - Balancing Engine

Copyright (c) 2018, InnoGames GmbH
"""

from os import environ

from adminapi.dataset import Query
from adminapi.filters import Any

from igvm.settings import (
    HYPERVISOR_ATTRIBUTES,
    HYPERVISOR_CONSTRAINTS,
    HYPERVISOR_RULES,
)
from igvm.balance.utils import filter_hypervisors, get_ranking
from igvm.hypervisor import Hypervisor


class Engine(object):
    def __init__(self, vm, hv_states=['online']):
        self.vm = vm
        self.possible_hypervisors = {
            obj['hostname']: Hypervisor(obj) for obj in Query({
                'servertype': 'hypervisor',
                'environment': environ.get('IGVM_MODE', 'production'),
                'vlan_networks': self.vm.dataset_obj['route_network'],
                'state': Any(*hv_states),
            }, HYPERVISOR_ATTRIBUTES)
        }

    def run(self):
        candidates = filter_hypervisors(
            self.vm, self.possible_hypervisors.values(), HYPERVISOR_CONSTRAINTS
        )

        ranking = get_ranking(self.vm, candidates, HYPERVISOR_RULES)

        for hostname in sorted(ranking, key=ranking.get, reverse=True):
            yield self.possible_hypervisors[hostname]
