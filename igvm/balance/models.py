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

    def get_memory_free(self, fast=False):
        """Get free memory for VMs in MiB -> float

        Get free memory in MiB from libvirt and return it or -1.0 on error.
        """

        if fast:
            vms_memory = float(sum([vm['memory'] for vm in self.get_vms()]))
            return float(self['memory']) - vms_memory
        else:
            ighv = KVMHypervisor(self.hostname)
            memory = float(ighv.free_vm_memory())
            close_virtconn(ighv.fqdn)

        return memory

    def get_disk_free(self, fast=False):
        """Get free disk size in MiB

        Get free disk size in MiB for VMs using igvm.

        :param: fast: Calculate disk by serveradmin value imprecise

        :return: float
        """

        if fast:
            # We reserved 10 GiB for root partition and 16 for swap.
            reserved = 16.0 + 10
            host = self['disk_size_gib']
            vms = float(sum(vm['disk_size_gib'] for vm in self.get_vms()))
            disk_free_mib = (host - vms - reserved) * 1024.0
        else:
            ighv = KVMHypervisor(self.hostname)
            gib = float(ighv.get_free_disk_size_gib())
            close_virtconn(ighv.fqdn)
            disk_free_mib = gib * 1024.0

        return disk_free_mib

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
