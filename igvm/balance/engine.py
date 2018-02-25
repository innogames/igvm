"""igvm - Balancing Engine

Copyright (c) 2018, InnoGames GmbH
"""

from os import environ

from adminapi.dataset import Query
from adminapi.filters import Any

from igvm.settings import HYPERVISOR_CONSTRAINTS, HYPERVISOR_RULES
from igvm.balance.models import VM, Hypervisor
from igvm.balance.utils import filter_hypervisors, get_ranking


class Engine(object):
    def __init__(self, vm_hostname, hv_states=['online']):
        self.vm = VM(Query({'hostname': vm_hostname}).get())
        if self.vm['xen_host']:
            self.vm.hypervisor = Hypervisor(
                Query({'hostname': self.vm['xen_host']}).get()
            )
        self.possible_hypervisors = {
            obj['hostname']: Hypervisor(obj) for obj in Query({
                'servertype': 'hypervisor',
                'environment': environ.get('IGVM_MODE', 'production'),
                'vlan_networks': self.vm['route_network'],
                'state': Any(*hv_states),
            })
        }

    def run(self):
        candidates = filter_hypervisors(
            self.vm, self.possible_hypervisors.values(), HYPERVISOR_CONSTRAINTS
        )

        ranking = get_ranking(self.vm, candidates, HYPERVISOR_RULES)

        for hostname in sorted(ranking, key=ranking.get, reverse=True):
            yield self.possible_hypervisors[hostname]
