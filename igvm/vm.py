import logging
import time

from igvm.host import Host
from igvm.hypervisor import Hypervisor
from igvm.utils.network import get_network_config


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

    def create(self, config):
        return self.hypervisor.create_vm(self, config)

    def start(self):
        log.debug('Starting {} on {}'.format(
            self.hostname, self.hypervisor.hostname))
        self.hypervisor.start_vm(self)
        if not self.wait_for_running(running=True):
            raise VMError('VM did not come online in time')

    def shutdown(self):
        log.debug('Stopping {} on {}'.format(
            self.hostname, self.hypervisor.hostname))
        self.hypervisor.stop_vm(self)
        if not self.wait_for_running(running=False):
            self.hypervisor.stop_vm_force(self)

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
