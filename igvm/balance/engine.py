"""igvm - Balancing Engine

Copyright (c) 2018, InnoGames GmbH
"""

import logging

from os import environ

from adminapi.dataset import Query
from adminapi.filters import Any

from igvm.settings import HYPERVISOR_CONSTRAINTS, HYPERVISOR_RULES
from igvm.balance.models import (
    VM,
    Hypervisor as HV,
)
from igvm.balance.utils import (
    filter_hypervisors,
    get_ranking,
    ServeradminCache
)


class Engine(object):
    def __init__(self, vm_hostname, hv_states=['online']):
        self._cache(vm_hostname)
        self.vm = VM(vm_hostname)
        self.hvs = [HV(host['hostname']) for host in Query({
            'servertype': 'hypervisor',
            'environment': environ.get('IGVM_MODE', 'production'),
            'vlan_networks': self.vm['route_network'],
            'state': Any(*hv_states),
        })]

    def _cache(self, vm_hostname):
        logging.info('Fetching serveradmin values for cache ...')

        vm = Query({'hostname': vm_hostname}).get()
        ServeradminCache.set(vm_hostname, vm)

        hvs = Query({
            'servertype': 'hypervisor',
            'vlan_networks': vm['route_network'],
            'environment': environ.get('IGVM_MODE', 'production')
        })
        for hv in hvs:
            ServeradminCache.set(hv['hostname'], hv)

        vms = Query({
            'servertype': 'vm',
            'xen_host': Any(*[hv['hostname'] for hv in hvs]),
        })
        for vm in vms:
            ServeradminCache.set(vm['hostname'], vm)

        logging.info('Cached {} objects'.format(ServeradminCache.size()))

    def run(self):
        candidates = filter_hypervisors(
            self.vm, self.hvs, HYPERVISOR_CONSTRAINTS
        )

        return get_ranking(self.vm, candidates, HYPERVISOR_RULES)
