"""igvm - Balancing Models

Copyright (c) 2018, InnoGames GmbH
"""

from adminapi.dataset import query
from adminapi.dataset.filters import Any

from igvm.hypervisor import Hypervisor as KVMHypervisor
from igvm.utils.virtutils import close_virtconn
from igvm.balance.utils import ServeradminCache as sc


class Game(object):
    """Game

    Represents a game and has convenience methods to access used Bladecenter,
    all VMs etc. of a game.

    :param: shortname: short name of the game as string

    :return:
    """

    def __init__(self, shortname):
        super(Game, self).__init__()
        self.shortname = shortname
        self._bladecenter = None
        self._hypervisors = None
        self._hypervisors_available = None
        self._vms = None

    def __eq__(self, other):
        if isinstance(other, Game):
            if self.shortname == other.shortname:
                return True
        return False

    def __hash__(self):
        return hash(self.shortname)

    def __str__(self):
        return self.shortname

    def __repr__(self):
        return '<balance.models.Game {}>'.format(self.shortname)

    def get_bladecenter(self):
        """Get all Bladecenter where at least one VM of game is in

        :return: list
        """

        if self._bladecenter is None:
            self._bladecenter = list(set(
                [hv.get_bladecenter() for hv in self.get_hypervisors()]
            ))

        return self._bladecenter

    def get_hypervisors(self):
        """Get Hypervisors where at least one VM of game is in:

        :return: list
        """

        if self._hypervisors is None:
            self._hypervisors = list(set(
                [vm.get_hypervisor() for vm in self.get_vms()]
            ))

        return self._hypervisors

    def get_hypervisors_available(self):
        """Get Hypervisors available for the game (not yet used)

        :return: list
        """

        if self._hypervisors_available is None:
            hvs = query(
                vlan_networks=Any(self.get_route_networks()),
                servertype='hypervisor',
                state='online'
            )
            hvs = set([hv['hostname'] for hv in hvs])
            self._hypervisors_available = [Hypervisor(hv) for hv in hvs]

        return self._hypervisors_available

    def get_route_networks(self):
        """Get route network

        :return: str
        """

        pns = sc.query(project=self.shortname, servertype='project_network')
        return list(set([pn['route_network'] for pn in pns]))

    def get_vms(self):
        """Get VMs of game

        :return: list
        """

        if self._vms is None:
            vms = sc.query(servertype='vm', project=self.shortname)
            self._vms = [VM(vm['hostname']) for vm in vms]

        return self._vms


class GameMarket(Game):
    """Game Market

    Represents a game market and allow to easily access all current VMs of a
    game market.

    :param: shortname: short name of the game as string
    """

    def __init__(self, shortname, market):
        super(GameMarket, self).__init__(shortname)
        self.market = market

    def __eq__(self, other):
        if isinstance(other, GameMarket):
            if self.shortname == other.shortname:
                if self.market == other.market:
                    return True
        return False

    def __hash__(self):
        return hash(self.shortname + self.market)

    def __str__(self):
        return self.shortname + self.market

    def __repr__(self):
        return '<balance.models.GameMarket {}{}>'.format(
            self.shortname, self.market)

    def get_vms(self):
        """Get VMs of game market

        :return: list
        """

        if self._vms is None:
            self._vms = []
            vms = sc.query(
                servertype='vm',
                project=self.shortname,
                game_market=self.market
            )
            self._vms = [VM(vm['hostname']) for vm in vms]

        return self._vms


class GameWorld(GameMarket):
    """Game World"""

    def __init__(self, shortname, market, world):
        super(GameWorld, self).__init__(shortname, market)
        self.world = world

    def __eq__(self, other):
        if isinstance(other, GameWorld):
            if self.shortname == other.shortname:
                if self.market == other.market:
                    if self.world == other.world:
                        return True
        return False

    def __hash__(self):
        return hash(self.shortname + self.market + self.world)

    def __str__(self):
        return self.shortname + self.market + self.world

    def __repr__(self):
        return '<balance.models.GameWorld {}{}{}>'.format(
            self.shortname, self.market, self.world)

    def get_vms(self):
        """Get VMs of game market

        :return: list
        """

        if self._vms is None:
            self._vms = []
            vms = sc.query(
                servertype='vm',
                project=self.shortname,
                game_market=self.market,
                game_world=self.world
            )
            self._vms = [VM(vm['hostname']) for vm in vms]

        return self._vms


class Bladecenter(object):
    """Bladecenter"""

    def __init__(self, bladecenter):
        super(Bladecenter, self).__init__()
        self.bladecenter = bladecenter
        self._hypervisors = None
        self._vms = None

    def __eq__(self, other):
        if self.bladecenter == other.bladecenter:
            return True
        else:
            return False

    def __hash__(self):
        return hash(self.bladecenter)

    def __str__(self):
        return self.bladecenter

    def __repr__(self):
        return '<balance.models.Bladecenter {}>'.format(self.bladecenter)

    def get_hypervisors(self):
        """get hypervisors -> []"""

        if self._hypervisors is None:
            self._hypervisors = []
            qs = sc.query(
                servertype='hypervisor', bladecenter=self.bladecenter
            )
            for hypervisor in qs:
                hostname = hypervisor['hostname']
                self._hypervisors.append(Hypervisor(hostname))

        return self._hypervisors

    def get_vms(self):
        """get vms -> []"""

        if self._vms is None:
            self._vms = []
            for hypervisor in self.get_hypervisors():
                self._vms.extend(hypervisor.get_vms())

        return self._vms

    def get_avg_load(self):
        """Get average load of hypervisors in bladecenter -> float

        Calculates the average load of the bladecenter for all hypervisors and
        returns the value as float or -1 if no hypervisors are available.
        """

        if len(self.get_hypervisors()) == 0:
            return -1.0

        load = 0.0
        for hv in self.get_hypervisors():
            load += hv.get_max_load()

        return float(load / len(self.get_hypervisors()))


class Host(object):
    """Host"""

    def __init__(self, hostname):
        self.hostname = hostname
        self._serveradmin_data = None

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
        self._fetch_serveradmin_data()
        return self._serveradmin_data[key]

    def keys(self):
        self._fetch_serveradmin_data()
        return self._serveradmin_data.keys()

    def _fetch_serveradmin_data(self):
        if self._serveradmin_data is None:
            self._serveradmin_data = sc.get(self.hostname)

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
    """Hypervisor"""

    def __init__(self, hostname):
        super(Hypervisor, self).__init__(hostname)
        self._bladecenter = None
        self._vms = None

    def __str__(self):
        return self.hostname

    def __repr__(self):
        return '<balance.models.Hypervisor {}>'.format(self.hostname)

    def get_state(self):
        """get hypervisor state -> str"""

        return self['state']

    def get_bladecenter(self):
        """get bladecenter -> Bladecenter"""

        if self._bladecenter is None:
            bladecenter = self['bladecenter']
            self._bladecenter = Bladecenter(bladecenter)

        return self._bladecenter

    def get_vms(self):
        """get vms -> []"""

        if self._vms is None:
            self._vms = []
            qs = sc.query(
                servertype='vm', xen_host=self.hostname
            )
            for vm in qs:
                self._vms.append(VM(vm['hostname']))

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
            vms = float(sum(
                [vm['disk_size_gib'] for vm in sc.query(
                    servertype='vm',
                    xen_host=self.hostname
                )]
            ))
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
        return self['cpu_util_vm_percent_max_week']

    def get_max_cpu_usage(self):
        """Get last 24h maximum CPU usage of 95 percentile for hypervisor

        Queries serveradmin graphite cache for the 95 percentile value of the
        maximum CPU usage of the hypervisor for the CPU usage and returns it.

        :return: float
        """
        return self['cpu_util_percent_max95_day']

    def get_max_load(self):
        """Get maximum load average of last 24h for hypervisor

        Queries serveradmin graphite cahce for the average load average of the
        last 24 hours and returns it.

        :return: float
        """
        return self['load_avg_day']


class VM(Host):
    """VM"""

    def __init__(self, hostname):
        super(VM, self).__init__(hostname)
        self._hypervisor = None

    def get_hypervisor(self):
        """get hypervisor -> Hypervisor"""

        if self._hypervisor is None:
            hostname = self['xen_host']
            self._hypervisor = Hypervisor(hostname)

        return self._hypervisor

    def get_bladecenter(self):
        """get bladecenter -> Bladecenter"""

        return self.get_hypervisor().get_bladecenter()

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
