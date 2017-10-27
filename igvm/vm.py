import os
import logging
import time

from base64 import b64decode
from fabric.api import cd, get, put, run, settings
from hashlib import sha1, sha256
from ipaddress import ip_address
from re import compile as re_compile
from StringIO import StringIO
from uuid import uuid1

from adminapi.dataset import query
from adminapi.dataset.filters import Any

from igvm.exceptions import (
    ConfigError,
    RemoteCommandError,
)
from igvm.host import Host
from igvm.hypervisor import Hypervisor
from igvm.utils.backoff import retry_wait_backoff
from igvm.settings import DEFAULT_SWAP_SIZE
from igvm.utils.cli import yellow
from igvm.utils.network import get_network_config
from igvm.utils.portping import wait_until
from igvm.utils.template import upload_template
from igvm.utils.transaction import run_in_transaction
from igvm.utils.units import parse_size
from igvm.balance.models import VM as BalanceVM
from igvm.balance.models import Hypervisor as BalanceHV
from igvm.balance.utils import (
    get_config,
    get_constraints,
    get_rules,
    filter_hypervisors,
    get_ranking,
)


log = logging.getLogger(__name__)


class VMError(Exception):
    pass


class VM(Host):
    """VM interface."""
    servertype = 'vm'

    def __init__(self, server_name_or_obj, ignore_reserved=False,
                 hypervisor=None):
        super(VM, self).__init__(server_name_or_obj, ignore_reserved)

        if not hypervisor:
            if not self.server_obj.get('xen_host'):
                self.server_obj.set('xen_host', self.find_hypervisor(
                    balance_ruleset='basic'))
                self.server_obj.commit()

            hypervisor = Hypervisor(
                self.server_obj['xen_host'], ignore_reserved
            )
        else:
            assert isinstance(hypervisor, Hypervisor)
        self.hypervisor = hypervisor

        # A flag to keep state of machine consistent between VM methods.
        # Operations on VM like run() or put() will use it to decide
        # upon method of accessing files correctly: mounted image on HV or
        # directly on running VM.
        self.mounted = False

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
            log.info(
                '"{0}" networking changed to IP address {1}, VLAN {2} ({3}).'
                .format(
                    self.fqdn,
                    new_ip,
                    self.network_config['vlan_name'],
                    self.network_config['vlan_tag'],
                )
            )

    def vm_host(self):
        """ Return correct ssh host for mounted and unmounted vm """

        if self.mounted:
            return self.hypervisor.fabric_settings()
        else:
            return self.fabric_settings()

    def vm_path(self, path=''):
        """ Append correct prefix to reach VM's / directory """

        if self.mounted:
            return '{}/{}'.format(
                self.hypervisor.vm_mount_path(self),
                path,
            )
        else:
            return '/{}'.format(path)

    def run(self, command, silent=False, with_sudo=True):
        """ Same as Fabric's run() but works on mounted or running vm

            When running in a mounted VM image, run everything in chroot
            and in separate shell inside chroot. Normally Fabric runs shell
            around commands.
        """
        with self.vm_host():
            if self.mounted:
                return self.hypervisor.run(
                    'chroot {} /bin/sh -c \'{}\''.format(
                        self.vm_path(''), command,
                    ),
                    shell=False, shell_escape=True,
                    silent=silent,
                    with_sudo=with_sudo,
                )
            else:
                return super(VM, self).run(command, silent=silent)

    def upload_template(self, filename, destination, context=None):
        """" Same as Fabric's template() but works on mounted or running vm """
        with self.vm_host():
            return upload_template(
                filename, self.vm_path(destination), context
            )

    def get(self, remote_path, local_path):
        """" Same as Fabric's get() but works on mounted or running vm """
        with self.vm_host():
            return get(self.vm_path(remote_path), local_path, temp_dir='/tmp')

    def put(self, remote_path, local_path, mode='0644'):
        """ Same as Fabric's put() but works on mounted or running vm

            Setting permissions on files and using sudo via Fabric's put()
            seems broken, at least for mounted VM. This is why we run
            extra commands here.
        """
        with self.vm_host():
            tempfile = '/tmp/' + str(uuid1())
            put(local_path, self.vm_path(tempfile))
            self.run('mv {0} {1} ; chmod {2} {1}'.format(
                tempfile, remote_path, mode
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
        """Validate relevant Serveradmin attributes"""

        mul_numa_nodes = 128 * self.hypervisor.num_numa_nodes()
        validations = [
            (
                'hostname',
                re_compile('\A[a-z][a-z0-9\.\-]+\Z').match,
                'invalid hostname',
            ),
            ('memory', lambda v: v > 0, 'memory must be > 0'),
            # https://medium.com/@juergen_thomann/memory-hotplug-with-qemu-kvm-and-libvirt-558f1c635972#.sytig6o9h
            (
                'memory',
                lambda v: v % mul_numa_nodes == 0,
                'memory must be multiple of {}MiB'.format(mul_numa_nodes),
            ),
            ('num_cpu', lambda v: v > 0, 'num_cpu must be > 0'),
            ('os', lambda v: True, 'os must be set'),
            (
                'disk_size_gib',
                lambda v: v > 0,
                'disk_size_gib must be > 0',
            ),
            ('puppet_ca', lambda v: True, 'puppet_ca must be set'),
            ('puppet_master', lambda v: True, 'puppet_master must be set'),
        ]

        for attr, check, err in validations:
            value = self.server_obj[attr]
            if not value:
                raise ConfigError('"{}" attribute is not set'.format(attr))
            if not check(value):
                raise ConfigError(err)

    def start(self, tx=None):
        self.hypervisor.start_vm(self)
        if not self.wait_for_running(running=True):
            raise VMError('VM did not come online in time')

        host_up = wait_until(
            str(self.server_obj['intern_ip']),
            waitmsg='Waiting for SSH to respond',
        )
        if not host_up:
            raise VMError('The server is not reachable with SSH')

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
            tx.on_rollback('stop VM', self.shutdown)

    def shutdown(self, tx=None):
        self.hypervisor.stop_vm(self)
        if not self.wait_for_running(running=False):
            self.hypervisor.stop_vm_force(self)

        if tx:
            tx.on_rollback('start VM', self.start)

    def is_running(self):
        return self.hypervisor.vm_running(self)

    def wait_for_running(self, running=True, timeout=60):
        """
        Waits for the VM to enter the given running state.
        Returns False on timeout, True otherwise.
        """
        action = 'boot' if running else 'shutdown'
        for i in range(timeout, 1, -1):
            print(
                'Waiting for VM "{}" to {}... {} s'
                .format(self.fqdn, action, i)
            )
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
                key, value = [tok.strip() for tok in line.split(':')]
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
            'hypervisor': self.hypervisor.fqdn,
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
        self.hypervisor.create_vm_storage(self, self.fqdn, tx)
        mount_path = self.hypervisor.format_vm_storage(self, tx)

        if not localimage:
            self.hypervisor.download_image(image)
        self.hypervisor.extract_image(image, mount_path)

        self.prepare_vm()

        if runpuppet:
            self.run_puppet(clear_cert=True, tx=tx)

        if postboot is not None:
            self.copy_postboot_script(postboot)

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
            self.run('rm /buildvm-postboot')

        log.info('"{}" is successfully built.'.format(self.fqdn))

    @run_in_transaction
    def rename(self, new_hostname, tx=None):
        """Rename the VM"""
        assert tx is not None, 'tx populated by run_in_transaction'

        new_fqdn = (
            new_hostname
            if new_hostname.endswith('.ig.local')
            else new_hostname + '.ig.local'
        )

        if new_fqdn == self.fqdn:
            raise ConfigError('The VM already named as "{}"'.format(self.fqdn))

        self.server_obj['hostname'] = new_hostname
        self.check_serveradmin_config()

        fd = StringIO(new_fqdn)
        self.put('/etc/hostname', fd)
        self.put('/etc/mailname', fd)

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
        self.hypervisor.rename_vm(self, new_hostname)

        self.server_obj.commit()

        self.start(tx=tx)

    def prepare_vm(self):
        """Prepare the rootfs for a VM

        VM storage must be mounted on the hypervisor.
        """
        fd = StringIO(self.fqdn)
        self.put('/etc/hostname', fd)
        self.put('/etc/mailname', fd)

        self.upload_template('etc/fstab', 'etc/fstab', {
            'blk_dev': self.hypervisor.vm_block_device_name(),
            'type': 'xfs',
            'mount_options': 'defaults'
        })
        self.upload_template('etc/hosts', '/etc/hosts')
        self.upload_template('etc/inittab', '/etc/inittab')

        # Copy resolv.conf from Hypervisor
        fd = StringIO()
        with self.hypervisor.fabric_settings(
                cd(self.hypervisor.vm_mount_path(self))
        ):
            get('/etc/resolv.conf', fd)
        self.put('/etc/resolv.conf', fd)

        self.create_swap(DEFAULT_SWAP_SIZE)
        self.create_ssh_keys()

    def create_ssh_keys(self):
        # If we wouldn't do remove those, ssh-keygen would ask us confirm
        # overwrite.
        self.run('rm -f /etc/ssh/ssh_host_*_key*')

        self.server_obj['sshfp'] = set()
        key_types = [(1, 'rsa'), (3, 'ecdsa')]
        if self.server_obj['os'] != 'wheezy':
            key_types.append((4, 'ed25519'))
        fp_types = [(1, sha1), (2, sha256)]

        # This will also create the public key files.
        for key_id, key_type in key_types:
            self.run(
                'ssh-keygen -q -t {0} -N "" '
                '-f /etc/ssh/ssh_host_{0}_key'
                .format(key_type)
            )

            fd = StringIO()
            self.get('/etc/ssh/ssh_host_{0}_key.pub'.format(key_type), fd)
            pub_key = b64decode(fd.getvalue().split(None, 2)[1])
            for fp_id, fp_type in fp_types:
                self.server_obj['sshfp'].add('{} {} {}'.format(
                    key_id, fp_id, fp_type(pub_key).hexdigest()
                ))

    def create_swap(self, size_MiB):
        self.run(
            'dd if=/dev/zero of=/swap bs=1M count={}'.format(size_MiB)
        )
        self.run('/bin/chmod 0600 /swap')
        self.run('/sbin/mkswap /swap')

    def run_puppet(self, clear_cert, tx):
        """Runs Puppet in chroot on the hypervisor."""

        if clear_cert:
            with settings(
                host_string=self.server_obj['puppet_ca'],
                user='root',
                warn_only=True,
            ):
                run(
                    '/usr/bin/puppet cert clean {}'.format(self.fqdn),
                    shell=False,
                )

            self.block_autostart()

        if tx:
            tx.on_rollback(
                'Kill puppet',
                self.run,
                'pkill -9 -f "/usr/bin/puppet agent -v --fqdn={}"'
                .format(self.fqdn)
            )
            self.run(
                '/usr/bin/puppet agent -v --fqdn={}'
                ' --server {} --ca_server {} --no-report'
                ' --waitforcert=60 --onetime --no-daemonize'
                ' --skip_tags=chroot_unsafe'
                ' && touch /tmp/puppet_success'
                ' | tee {} ;'
                ' test -f /tmp/puppet_success'
                .format(
                    self.fqdn,
                    self.server_obj['puppet_master'],
                    self.server_obj['puppet_ca'],
                    '/var/log/puppetrun_igvm',
                )
            )

        self.unblock_autostart()

    def block_autostart(self):
        fd = StringIO('#!/bin/sh\nexit 101\n')
        self.put('/usr/sbin/policy-rc.d', fd, '0755')

    def unblock_autostart(self):
        self.run('rm /usr/sbin/policy-rc.d')

    def copy_postboot_script(self, script):
        self.put('/buildvm-postboot', script, '0755')

    def find_hypervisor(self, states=['online'], balance_ruleset=None):
        """Find a hypervisor where VM can be migrated to

        Tries to find a better hypervisor for the VM and return the hostname of
        the new hypervisor. It returns None if no hypervisor is available.

        :param: states: hypervisor states as list
        :param: balance_ruleset: force using this balance rule set

        :return: str|None
        """

        filters = {
            'servertype': 'hypervisor',
            'state': Any(*states),
            'vlan_networks': self.server_obj['route_network'],
        }
        if 'IGVM_MODE' in os.environ and os.environ['IGVM_MODE'] == 'testing':
            filters['environment'] = 'testing'

        hvs = [
            BalanceHV(host['hostname'])
            for host in query(**filters).restrict('hostname')
        ]
        vm = BalanceVM(self.server_obj['hostname'])

        logging.info('Testing {} hypervisors to find the best one'.format(
            len(hvs)
        ))

        if balance_ruleset:
            logging.info('using ruleset {} for initial setup'.format(
                balance_ruleset))
            config = get_config(balance_ruleset)
        else:
            config = get_config(self.server_obj['project'])

        constraints = get_constraints(config['constraints'])
        hvs = filter_hypervisors(vm, hvs, constraints)

        if not hvs:
            return None

        rules = get_rules(config['rules'])
        ranking = get_ranking(vm, hvs, rules)
        best_hvs = sorted(ranking, key=ranking.get, reverse=True)

        for hv in best_hvs:
            logging.debug('{}: {}'.format(hv, ranking[hv]))

        hv_index = 0
        cur_hv = None
        if 'xen_host' in self.server_obj:
            cur_hv = self.server_obj['xen_host']

        # We check disk size of hypervisor in a quick a dirty way for know
        # because running SSH to each host takes forever. This can return wrong
        # values so we better check before returning the best hypervisor if it
        # has already enough disk space free.
        for hv in best_hvs:
            if hv_index >= len(best_hvs):
                return None

            bhv = Hypervisor(best_hvs[hv_index]).get_free_disk_size_gib()
            vm = float(self.server_obj['disk_size_gib'])

            if bhv > vm and cur_hv != best_hvs[hv_index]:
                return best_hvs[hv_index]
            else:
                hv_index += 1
