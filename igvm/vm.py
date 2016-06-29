import logging
import time

from igvm.exceptions import (
    ConfigError,
    RemoteCommandError,
)
from igvm.host import Host
from igvm.hypervisor import Hypervisor
from igvm.utils.network import get_network_config
from igvm.utils.portping import wait_until
from igvm.utils.units import parse_size

log = logging.getLogger(__name__)


class VMError(Exception):
    pass


class VM(Host):
    """VM interface."""
    def __init__(self, vm_admintool, hv=None):
        super(VM, self).__init__(vm_admintool, servertype='vm')

        if not hv:
            hv = Hypervisor.get(self.admintool['xen_host'])
        assert isinstance(hv, Hypervisor)
        self.hypervisor = hv

    def _set_ip(self, new_ip):
        """Changes the IP address and updates all related attributes.
        Internal method for VM building and migration."""
        old_ip = self.admintool['intern_ip']
        self.admintool['intern_ip'] = new_ip
        self.network_config = get_network_config(self.admintool)
        self.admintool['segment'] = self.network_config['segment']

        if old_ip != new_ip:
            log.info((
                '{0} networking changed: '
                'Segment {1}, IP address {2}, VLAN {3}')
                .format(
                    self.hostname,
                    self.admintool['segment'],
                    new_ip,
                    self.network_config['vlan'],
            ))

    def set_num_cpu(self, num_cpu):
        """Changes the number of CPUs."""
        self.hypervisor.vm_set_num_cpu(self, num_cpu)

    def set_memory(self, memory):
        """Resizes the host memory."""
        self.hypervisor.vm_set_memory(self, memory)

    def check_serveradmin_config(self):
        """Validates relevant serveradmin attributes."""
        validations = (
            ('memory', (lambda v: v > 0, 'memory must be > 0')),
            ('num_cpu', (lambda v: v > 0, 'num_cpu must be > 0')),
            ('os', (lambda v: True, 'os must be set')),
            (
                'disk_size_gib',
                (lambda v: v > 0, 'disk_size_gib must be > 0')
            ),
        )

        for (attr, (check, err)) in validations:
            value = self.admintool[attr]
            if not value:
                raise ConfigError('"{}" attribute is not set'.format(attr))
            if not check(value):
                raise ConfigError(err)

    def start(self):
        log.debug('Starting {} on {}'.format(
            self.hostname, self.hypervisor.hostname))
        self.hypervisor.start_vm(self)
        if not self.wait_for_running(running=True):
            raise VMError('VM did not come online in time')

        host_up = wait_until(
            str(self.admintool['intern_ip']),
            waitmsg='Waiting for SSH server',
        )
        if not host_up:
            raise VMError('SSH server is not reachable via TCP')

        # Wait until we can login
        log.info('Trying SSH login')
        for i in range(0, 7):
            try:
                self.run('ls', silent=True)
                break
            except Exception as e:
                pass
            sleep_time = 0.1 * 2**i
            log.info('Failed, retrying in {:.2f}s'.format(sleep_time))
            time.sleep(sleep_time)
        else:
            raise VMError('SSH server does not allow login: {}'.format(e))

    def shutdown(self):
        log.debug('Stopping {} on {}'.format(
            self.hostname, self.hypervisor.hostname))
        self.hypervisor.stop_vm(self)
        if not self.wait_for_running(running=False):
            self.hypervisor.stop_vm_force(self)
        self.disconnect()

    def is_running(self):
        return self.hypervisor.vm_running(self)

    def undefine(self):
        log.debug('Undefining {} on {}'.format(
            self.hostname, self.hypervisor.hostname))
        self.hypervisor.undefine_vm(self)

    def wait_for_running(self, running=True, timeout=60):
        """
        Waits for the VM to enter the given running state.
        Returns False on timeout, True otherwise.
        """
        action = 'boot' if running else 'shutdown'
        for i in range(timeout, 1, -1):
            print("Waiting for VM {} to {}... {}s".format(
                self.hostname, action, i))
            if self.hypervisor.vm_running(self) == running:
                return True
            time.sleep(1)
        else:
            return False

    def meminfo(self):
        """Returns a dictionary of /proc/meminfo entries."""
        contents = self.read_file('/proc/meminfo')
        result = {}
        for line in contents.splitlines():
            try:
                (key, value) = [tok.strip() for tok in line.split(':')]
            except IndexError:
                continue
            result[key] = value
        return result

    def memory_free(self):
        meminfo = self.meminfo()

        if 'MemAvailable' in meminfo:
            kib_free = parse_size(meminfo['MemAvailable'], 'K')
        # MemAvailable might not be present on old systems
        elif 'MemFree' in meminfo:
            kib_free = parse_size(meminfo['MemFree'], 'K')
        else:
            raise VMError('/proc/meminfo contains no parsable entries')

        return round(float(kib_free) / 1024, 2)

    def disk_free(self):
        """Returns free disk space in GiB"""
        output = self.run(
            "df -k / | tail -n+2 | awk '{ print $4 }'",
            silent=True,
        ).strip()
        if not output.isdigit():
            raise RemoteCommandError('Non-numeric output in disk_free')
        return round(float(output) / 1024**2, 2)

    def info(self):
        result = {
            'hypervisor': self.hypervisor.hostname,
            'intern_ip': self.admintool['intern_ip'],
            'num_cpu': self.admintool['num_cpu'],
            'memory': self.admintool['memory'],
            'disk_size_gib': self.admintool['disk_size_gib'],
        }

        if self.hypervisor.vm_defined(self) and self.is_running():
            result.update(self.hypervisor.vm_sync_from_hypervisor(self))
            result.update({
                'status': 'running',
                'memory_free': self.memory_free(),
                'disk_free_gib': self.disk_free(),
                'load': self.read_file('/proc/loadavg').split()[:3],
            })
            result.update(self.hypervisor.vm_info(self))
        elif self.hypervisor.vm_defined(self):
            result['status'] = 'stopped'
        else:
            result['status'] = 'new'
        return result
