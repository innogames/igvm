"""igvm - Balancing Models

Copyright (c) 2018, InnoGames GmbH
"""

from adminapi.dataset import Query

from igvm.hypervisor import Hypervisor as KVMHypervisor
from igvm.utils.virtutils import close_virtconn


class Host(object):
    def __init__(self, obj):
        self.hostname = obj['hostname']
        self._serveradmin_data = obj

    def __eq__(self, other):
        if isinstance(other, Host):
            if self.hostname == other.hostname:
                return True
        else:
            return False

    def __hash__(self):
        return hash(self.hostname)

    def __str__(self):
        return self.hostname

    def __repr__(self):
        return '<balance.models.Host {}>'.format(self.hostname)

    def __getitem__(self, key):
        return self._serveradmin_data[key]

    def keys(self):
        return self._serveradmin_data.keys()

    def get_memory(self):
        """get available or allocated memory in MiB -> int"""

        return int(self['memory'])

    def get_cpus(self):
        """get available or allocated cpus -> int"""

        return int(self['num_cpu'])

    def get_disk_size(self):
        """Get allocated disk size in MiB -> int"""

        return int(self['disk_size_gib'] * 1024)


class Hypervisor(Host):
    def __init__(self, obj):
        super(Hypervisor, self).__init__(obj)
        self._vms = None

    def __str__(self):
        return self.hostname

    def __repr__(self):
        return '<balance.models.Hypervisor {}>'.format(self.hostname)

    def get_state(self):
        """get hypervisor state -> str"""

        return self['state']

    def get_vms(self):
        if self._vms is None:
            self._vms = [VM(o, self) for o in Query({
                'servertype': 'vm',
                'xen_host': self.hostname,
            })]

        return self._vms

    def get_memory_free(self):
        """Get free memory for VMs in MiB"""
        ighv = KVMHypervisor(self.hostname)
        try:
            return ighv.free_vm_memory()
        finally:
            close_virtconn(ighv.fqdn)

    def get_disk_free(self):
        """Get free disk size in GiB"""
        ighv = KVMHypervisor(self.hostname)
        try:
            return ighv.get_free_disk_size_gib()
        finally:
            close_virtconn(ighv.fqdn)

    def get_max_vcpu_usage(self):
        """Get last 24h maximum vCPU usage of 95 percentile for hypervisor

        Queries serveradmin graphite cache for the 95 percentile value of the
        maximum CPU usage of the hypervisor for the CPU usage and returns it.

        :return: float
        """
        return self['cpu_util_vm_pct']

    def get_max_cpu_usage(self):
        """Get last 24h maximum CPU usage of 95 percentile for hypervisor

        Queries serveradmin graphite cache for the 95 percentile value of the
        maximum CPU usage of the hypervisor for the CPU usage and returns it.

        :return: float
        """
        return self['cpu_util_pct']

    def get_max_load(self):
        """Get maximum load average of last 24h for hypervisor

        Queries serveradmin graphite cahce for the average load average of the
        last 24 hours and returns it.

        :return: float
        """
        return self['load_avg_day']


class VM(Host):
    def __init__(self, obj, hypervisor=None):
        super(VM, self).__init__(obj)
        self.hypervisor = hypervisor

    def get_identifier(self):
        """get game identifer for vm -> str"""

        if self['game_market'] and self['game_world']:
            identifier = '{}-{}-{}'.format(
                self['project'],
                self['game_market'],
                self['game_world'],
            )

            if self['game_type']:
                identifier += (
                    '-' + self['game_type']
                )

            return identifier
        else:
            return '{}-{}-{}'.format(
                self['project'],
                self['function'],
                self['environment']
            )
