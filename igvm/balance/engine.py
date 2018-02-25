"""igvm - Balancing Engine

Copyright (c) 2018, InnoGames GmbH
"""

import logging

from os import environ

from adminapi.dataset import Query
from adminapi.filters import Any

from igvm.balance.models import (
    VM,
    Hypervisor as HV,
)
from igvm.balance.utils import (
    get_config,
    get_constraints,
    get_rules,
    filter_hypervisors,
    get_ranking,
    ServeradminCache
)


class Engine(object):
    def __init__(self, vm_hostname, config, hv_states=['online']):
        self._cache(vm_hostname)
        self.vm = VM(vm_hostname)
        self.config = get_config(config or self.vm['project'])
        self.constraints = get_constraints(self.config['constraints'])
        self.rules = get_rules(self.config['rules'])
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
        candidates = filter_hypervisors(self.vm, self.hvs, self.constraints)
        if not candidates:
            return None

        return get_ranking(self.vm, candidates, self.rules)
