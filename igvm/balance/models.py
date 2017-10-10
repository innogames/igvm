import adminapi.api

from adminapi.dataset import query
from adminapi.dataset.filters import Any

from igvm.hypervisor import Hypervisor as KVMHypervisor
from igvm.utils.virtutils import close_virtconn


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
        return self.shortname

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
            hvs = query(vlan_networks=Any(self.get_route_networks()),
                        servertype='hypervisor',
                        state='online')
            hvs = set([hv['hostname'] for hv in hvs])
            self._hypervisors_available = [Hypervisor(hv) for hv in hvs]

        return self._hypervisors_available

    def get_route_networks(self):
        """Get route network

        :return: str
        """

        pns = query(project=self.shortname, servertype='project_network')
        return list(set([pn['route_network'] for pn in pns]))

    def get_vms(self):
        """Get VMs of game

        :return: list
        """

        if self._vms is None:
            vms = query(servertype='vm', project=self.shortname)
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
        return self.shortname + self.market

    def get_vms(self):
        """Get VMs of game market

        :return: list
        """

        if self._vms is None:
            self._vms = []
            vms = query(servertype='vm', project=self.shortname,
                        game_market=self.market)
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
        return self.shortname + self.market + self.world

    def get_vms(self):
        """Get VMs of game market

        :return: list
        """

        if self._vms is None:
            self._vms = []
            vms = query(servertype='vm', project=self.shortname,
                        game_market=self.market, game_world=self.world)
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
        return self.bladecenter

    def get_hypervisors(self):
        """get hypervisors -> []"""

        if self._hypervisors is None:
            self._hypervisors = []
            qs = query(servertype='hypervisor', bladecenter=self.bladecenter)
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

    def get_avg_load(self, tfrom='-1w', tuntil='now'):
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
        super(Host, self).__init__()
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
        return self.hostname

    def get_serveradmin_data(self):
        """get serveradmin object data -> dict"""

        if self._serveradmin_data is None:
            self._serveradmin_data = query(hostname=self.hostname).get()

        return self._serveradmin_data

    def get_memory(self):
        """get available or allocated memory in MiB -> int"""

        return int(self.get_serveradmin_data()['memory'])

    def get_cpus(self):
        """get available or allocated cpus -> int"""

        return int(self.get_serveradmin_data()['num_cpu'])

    def get_disk_size(self):
        """Get allocated disk size in MiB -> int"""

        return int(self.get_serveradmin_data()['disk_size_gib'] * 1024)


class Hypervisor(Host):
    """Hypervisor"""

    def __init__(self, hostname):
        super(Hypervisor, self).__init__(hostname)
        self._graphite_data = None
        self._bladecenter = None
        self._vms = None

    def get_state(self):
        """get hypervisor state -> str"""

        return self.get_serveradmin_data()['state']

    def get_bladecenter(self):
        """get bladecenter -> Bladecenter"""

        if self._bladecenter is None:
            bladecenter = self.get_serveradmin_data()['bladecenter']
            self._bladecenter = Bladecenter(bladecenter)

        return self._bladecenter

    def get_vms(self):
        """get vms -> []"""

        if self._vms is None:
            self._vms = []
            qs = query(servertype='vm', xen_host=self.hostname)
            for vm in qs:
                self._vms.append(VM(vm['hostname']))

        return self._vms

    def get_memory_free(self):
        """Get free memory for VMs in MiB -> float

        Get free memory in MiB from libvirt and return it or -1.0 on error.
        """

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

        disk_free_mib = 0.0

        if fast:
            # We reserved 10 GiB for root partition and 16 for swap.
            reserved = 16.0 + 10
            host = self.get_serveradmin_data()['disk_size_gib']
            vms = float(sum(
                [vm['disk_size_gib'] for vm in query(
                    servertype='vm',
                    xen_host=self.hostname
                ).restrict('disk_size_gib')]
            ))
            disk_free_mib = (host - vms - reserved) * 1024.0
        else:
            ighv = KVMHypervisor(self.hostname)
            gib = float(ighv.get_free_disk_size_gib())
            close_virtconn(ighv.fqdn)
            disk_free_mib = gib * 1024.0

        return disk_free_mib

    def get_graphite_data(self):
        """Get graphite numeric cache data from serveradmin

        :return: dict
        """

        if self._graphite_data is None:
            api = adminapi.api.get('graphite')
            self._graphite_data = api.get_numeric_cache(self.hostname)

        return self._graphite_data

    def get_max_vcpu_usage(self):
        """Get last 24h maximum vCPU usage of 95 percentile for hypervisor

        Queries serveradmin graphite cache for the 95 percentile value of the
        maximum CPU usage of the hypervisor for the CPU usage and returns it.

        :return: float
        """

        alias = 'Max vCPU Week'
        cache = (t for t in self.get_graphite_data() if t['template'] == alias)

        try:
            return float(cache.next()['value'])
        except StopIteration:
            return 0.0

    def get_max_cpu_usage(self):
        """Get last 24h maximum CPU usage of 95 percentile for hypervisor

        Queries serveradmin graphite cache for the 95 percentile value of the
        maximum CPU usage of the hypervisor for the CPU usage and returns it.

        :return: float
        """

        alias = 'Max95 CPU Day'
        cache = (t for t in self.get_graphite_data() if t['template'] == alias)

        try:
            return float(cache.next()['value'])
        except StopIteration:
            return 0.0

    def get_max_load(self):
        """Get maximum load average of last 24h for hypervisor

        Queries serveradmin graphite cahce for the average load average of the
        last 24 hours and returns it.

        :return: float
        """

        alias = 'Load AVG Day'
        cache = (t for t in self.get_graphite_data() if t['template'] == alias)

        try:
            return float(cache.next()['value'])
        except StopIteration:
            return 0.0


class VM(Host):
    """VM"""

    def __init__(self, hostname):
        super(VM, self).__init__(hostname)
        self._hypervisor = None

    def get_hypervisor(self):
        """get hypervisor -> Hypervisor"""

        if self._hypervisor is None:
            hostname = self.get_serveradmin_data()['xen_host']
            self._hypervisor = Hypervisor(hostname)

        return self._hypervisor

    def get_bladecenter(self):
        """get bladecenter -> Bladecenter"""

        return self.get_hypervisor().get_bladecenter()

    def get_game(self):
        """get game shortname vm is assinged to -> str"""

        return self.get_serveradmin_data()['project']

    def get_domain(self):
        """get domain for game -> str"""

        return self.get_serveradmin_data()['project']

    def get_market(self):
        """get market shortcode vm is assinged to -> str"""

        return str(self.get_serveradmin_data()['game_market'])

    def get_world(self):
        """get world shortcode vm is assinged to -> str"""

        return str(self.get_serveradmin_data()['game_world'])

    def get_function(self):
        """get game function -> str"""

        return str(self.get_serveradmin_data()['function'])

    def get_identifier(self):
        """get game identifer for vm -> str"""

        data = self.get_serveradmin_data()
        if 'game_market' in data and 'game_world' in data:
            domain = self.get_domain()
            market = self.get_market()
            world = self.get_world()

            identifier = domain + '-' + market + '-' + world

            return identifier

        return 'unknown identifier'
