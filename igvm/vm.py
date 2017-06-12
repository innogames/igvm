import logging
import time
from ipaddress import ip_address

from igvm.exceptions import (
    ConfigError,
    RemoteCommandError,
)
from igvm.host import Host
from igvm.hypervisor import Hypervisor
from igvm.utils.backoff import retry_wait_backoff
from igvm.utils.cli import yellow
from igvm.utils.image import download_image, extract_image
from igvm.utils.network import get_network_config
from igvm.utils.portping import wait_until
from igvm.utils.preparevm import (
    prepare_vm,
    copy_postboot_script,
    run_puppet,
)
from igvm.utils.transaction import run_in_transaction
from igvm.utils.units import parse_size

log = logging.getLogger(__name__)


class VMError(Exception):
    pass


class VM(Host):
    """VM interface."""
    servertype = 'vm'

    def __init__(self, vm_server_obj, hypervisor=None, ignore_reserved=False):
        super(VM, self).__init__(vm_server_obj)

        if not hypervisor:
            hypervisor = Hypervisor.get(
                self.server_obj['xen_host'], ignore_reserved
            )
        else:
            assert isinstance(hypervisor, Hypervisor)
        self.hypervisor = hypervisor

    def _set_ip(self, new_ip):
        """Changes the IP address and updates all related attributes.
        Internal method for VM building and migration."""
        old_ip = self.server_obj['intern_ip']
        # New IP address is given as a string,
        # server_obj['intern_ip'] is ip_address!
        # So convert the type.
        self.server_obj['intern_ip'] = ip_address(new_ip)
        self.network_config = get_network_config(self.server_obj)

        if old_ip != new_ip:
            log.info((
                '{0} networking changed: '
                'IP address {1}, VLAN {2} ({3})')
                .format(
                    self.hostname,
                    new_ip,
                    self.network_config['vlan_name'],
                    self.network_config['vlan_tag'],
            ))

    def set_state(self, new_state, tx=None):
        """Changes state of VM for LB and Nagios downtimes"""
        self.previous_state = self.server_obj['state']
        if new_state == self.previous_state:
            return
        log.debug('Setting VM to state {}'.format(new_state))
        self.server_obj['state'] = new_state
        self.server_obj.commit()
        if tx:
            tx.on_rollback('reset_state', self.reset_state)

    def reset_state(self):
        """Change state of VM to the original one"""
        # Transaction is not necessary here, because reverting it
        # would set the value to the original one anyway.
        if hasattr(self, 'previous_state'):
            self.set_state(self.previous_state)

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
            ('puppet_ca', (lambda v: True, 'puppet_ca must be set')),
            ('puppet_master', (lambda v: True, 'puppet_master must be set')),
        )

        for (attr, (check, err)) in validations:
            value = self.server_obj[attr]
            if not value:
                raise ConfigError('"{}" attribute is not set'.format(attr))
            if not check(value):
                raise ConfigError(err)

    def start(self, hypervisor=None, tx=None):
        hypervisor = hypervisor or self.hypervisor
        log.debug(
            'Starting {} on {}'.format(self.hostname, hypervisor.hostname)
        )
        hypervisor.start_vm(self)
        if not self.wait_for_running(running=True):
            raise VMError('VM did not come online in time')

        host_up = wait_until(
            str(self.server_obj['intern_ip']),
            waitmsg='Waiting for SSH server',
        )
        if not host_up:
            raise VMError('SSH server is not reachable via TCP')

        # Wait until we can login
        log.info('Trying SSH login')

        def _try_login():
            try:
                self.run('ls', silent=True)
                return True
            except Exception:
                pass
            return False

        retry_wait_backoff(
            _try_login,
            'SSH login failed',
        )

        if tx:
            tx.on_rollback('stop VM', self.shutdown, hypervisor)

    def shutdown(self, hypervisor=None, tx=None):
        hypervisor = hypervisor or self.hypervisor
        log.debug(
            'Stopping {} on {}'.format(self.hostname, hypervisor.hostname)
        )
        hypervisor.stop_vm(self)
        if not self.wait_for_running(running=False):
            hypervisor.stop_vm_force(self)
        self.disconnect()

        if tx:
            tx.on_rollback('start VM', self.start, self.hypervisor)

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
            'intern_ip': self.server_obj['intern_ip'],
            'num_cpu': self.server_obj['num_cpu'],
            'memory': self.server_obj['memory'],
            'disk_size_gib': self.server_obj['disk_size_gib'],
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

    @run_in_transaction
    def build(self, localimage=None, runpuppet=True, postboot=None, tx=None):
        """Builds a VM."""
        assert tx is not None, 'tx populated by run_in_transaction'

        hypervisor = self.hypervisor
        self.check_serveradmin_config()

        if localimage is not None:
            image = localimage
        else:
            image = self.server_obj['os'] + '-base.tar.gz'

        # Populate initial networking attributes.
        self._set_ip(self.server_obj['intern_ip'])

        # Can VM run on given hypervisor?
        self.hypervisor.check_vm(self)

        if not runpuppet or self.server_obj['puppet_disabled']:
            log.warn(yellow(
                'Puppet is disabled on the VM.  It will not receive network '
                'configuration.  Expect things to go south.'
            ))

        # Perform operations on the hypervisor
        self.hypervisor.create_vm_storage(self, tx)
        mount_path = self.hypervisor.format_vm_storage(self, tx)

        with hypervisor.fabric_settings():
            if not localimage:
                download_image(image)
            extract_image(image, mount_path, hypervisor.server_obj['os'])

        prepare_vm(hypervisor, self)

        if runpuppet:
            run_puppet(hypervisor, self, clear_cert=True, tx=tx)

        if postboot is not None:
            copy_postboot_script(hypervisor, self, postboot)

        self.hypervisor.umount_vm_storage(self)
        hypervisor.define_vm(self, tx)

        # We are updating the information on the Serveradmin, before starting
        # the VM, because the VM would still be on the hypervisor even if it
        # fails to start.
        self.server_obj.commit()

        # VM was successfully built, don't risk undoing all this just because
        # start fails.
        tx.checkpoint()

        self.start()

        # Perform operations on Virtual Machine
        if postboot is not None:
            self.run('/buildvm-postboot')
            self.run('rm -f /buildvm-postboot')

        log.info('{} successfully built.'.format(self.hostname))

    @run_in_transaction
    def rename(self, new_hostname, tx=None):
        """Rename the VM"""
        assert tx is not None, 'tx populated by run_in_transaction'

        new_fqdn = (
            new_hostname
            if new_hostname.endswith('.ig.local')
            else new_hostname + '.ig.local'
        )

        self.run('echo {0} > /etc/hostname'.format(new_fqdn))
        self.run('echo {0} > /etc/mailname'.format(new_fqdn))

        hosts_file = [
            line
            for line in self.run('cat /etc/hosts').splitlines()
            if not line.startswith(str(self.server_obj['intern_ip']))
        ]
        hosts_file.append('{0}\t{1}\t{2}'.format(
            self.server_obj['intern_ip'], new_fqdn, new_hostname
        ))
        self.run("echo '{0}' > /etc/hosts".format('\n'.join(hosts_file)))

        self.shutdown(tx=tx)
        self.hypervisor.undefine_vm(self)
        self.hypervisor.rename_vm_storage(self, new_hostname)

        self.server_obj['hostname'] = new_hostname
        self.server_obj.commit()

        new = VM(new_hostname)
        new.hypervisor.define_vm(new, tx=tx)
        new.start(tx=tx)
