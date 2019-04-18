"""igvm - VM Model

Copyright (c) 2018 InnoGames GmbH
"""

import logging
import os
import time

from base64 import b64decode
from fabric.api import cd, get, put, run, settings
from fabric.contrib.files import upload_template
from hashlib import sha1, sha256
from io import BytesIO
from re import compile as re_compile
from uuid import uuid4

from igvm.exceptions import ConfigError, RemoteCommandError, VMError
from igvm.host import Host
from igvm.transaction import Transaction
from igvm.utils import parse_size, wait_until

log = logging.getLogger(__name__)


class VM(Host):
    """VM interface."""
    servertype = 'vm'

    def __init__(self, dataset_obj, hypervisor=None):
        super(VM, self).__init__(dataset_obj)
        self.hypervisor = hypervisor

        # A flag to keep state of machine consistent between VM methods.
        # Operations on VM like run() or put() will use it to decide
        # upon method of accessing files correctly: mounted image on HV or
        # directly on running VM.
        self.mounted = False

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

    def read_file(self, path):
        """Read a file from a running VM or a mounted image on HV."""
        with self.vm_host():
            return super(VM, self).read_file(self.vm_path(path))

    def upload_template(self, filename, destination, context=None):
        """" Same as Fabric's template() but works on mounted or running vm """
        template_dir = os.path.join(os.path.dirname(__file__), 'templates')
        with self.vm_host():
            return upload_template(
                filename,
                self.vm_path(destination),
                context,
                backup=False,
                use_jinja=True,
                template_dir=template_dir,
                use_sudo=True,
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
            tempfile = '/tmp/' + str(uuid4())
            put(local_path, self.vm_path(tempfile))
            self.run('mv {0} {1} ; chmod {2} {1}'.format(
                tempfile, remote_path, mode
            ))

    def set_state(self, new_state, transaction=None):
        """Changes state of VM for LB and Nagios downtimes"""
        self.previous_state = self.dataset_obj['state']
        if self.previous_state == 'retired':
            # Don't set a state closer to online if VM is retired
            return
        if new_state == self.previous_state:
            return
        log.debug('Setting VM to state {}'.format(new_state))
        self.dataset_obj['state'] = new_state
        self.dataset_obj.commit()
        if transaction:
            transaction.on_rollback('reset_state', self.reset_state)

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
                re_compile(r'\A[a-z][a-z0-9\.\-]+\Z').match,
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
            value = self.dataset_obj[attr]
            if not value:
                raise ConfigError('"{}" attribute is not set'.format(attr))
            if not check(value):
                raise ConfigError(err)

    def start(self, force_stop_failed=True, transaction=None):
        self.hypervisor.start_vm(self)
        if not self.wait_for_running(running=True):
            raise VMError('VM did not come online in time')

        host_up = wait_until(
            str(self.dataset_obj['intern_ip']),
            waitmsg='Waiting for SSH to respond',
        )
        if not host_up and force_stop_failed:
            # If there is a network or booting error VM must be destroyed
            # if starting has failed.
            self.hypervisor.stop_vm_force(self)
        if not host_up:
            raise VMError('The server is not reachable with SSH')

        if transaction:
            transaction.on_rollback('stop VM', self.shutdown)

    def shutdown(self, check_vm_up_on_transaction=True, transaction=None):
        self.hypervisor.stop_vm(self)
        if not self.wait_for_running(running=False):
            self.hypervisor.stop_vm_force(self)

        if transaction:
            transaction.on_rollback(
                'start VM', self.start,
                force_stop_failed=check_vm_up_on_transaction,
            )

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
            # XXX: What are we really expecting in here?
            try:
                key, value = map(str.strip, line.decode().split(':'))
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
            'intern_ip': self.dataset_obj['intern_ip'],
            'num_cpu': self.dataset_obj['num_cpu'],
            'memory': self.dataset_obj['memory'],
            'disk_size_gib': self.dataset_obj['disk_size_gib'],
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

    def build(self, run_puppet=True, debug_puppet=False, postboot=None):
        """Builds a VM."""
        hypervisor = self.hypervisor
        self.check_serveradmin_config()

        image = self.dataset_obj['os'] + '-base.tar.gz'

        # Can VM run on given hypervisor?
        self.hypervisor.check_vm(self, offline=True)

        if not run_puppet or self.dataset_obj['puppet_disabled']:
            log.warn(
                'Puppet is disabled on the VM.  It will not receive network '
                'configuration.  Expect things to go south.'
            )

        with Transaction() as transaction:
            # Perform operations on the hypervisor
            self.hypervisor.create_vm_storage(self, transaction)
            mount_path = self.hypervisor.format_vm_storage(self, transaction)

            self.hypervisor.download_and_extract_image(image, mount_path)

            self.prepare_vm()

            if run_puppet:
                self.run_puppet(clear_cert=True, debug=debug_puppet)

            if postboot is not None:
                self.copy_postboot_script(postboot)

            self.hypervisor.umount_vm_storage(self)
            hypervisor.define_vm(self, transaction)

            # We are updating the information on the Serveradmin, before
            # starting the VM, because the VM would still be on the hypervisor
            # even if it fails to start.
            self.dataset_obj.commit()

            self.start()

        # Perform operations on Virtual Machine
        if postboot is not None:
            self.run('/buildvm-postboot')
            self.run('rm /buildvm-postboot')

        log.info('"{}" is successfully built.'.format(self.fqdn))

    def rename(self, new_hostname):
        """Rename the VM"""
        self.dataset_obj['hostname'] = new_hostname
        self.check_serveradmin_config()

        fd = BytesIO()
        fd.write(bytes(new_hostname, 'utf-8'))
        self.put('/etc/hostname', fd)
        self.put('/etc/mailname', fd)

        hosts_file = [
            line
            for line in self.run('cat /etc/hosts').splitlines()
            if not line.startswith(str(self.dataset_obj['intern_ip']))
        ]
        hosts_file.append('{0}\t{1}'.format(
            self.dataset_obj['intern_ip'], new_hostname
        ))
        self.run("echo '{0}' > /etc/hosts".format('\n'.join(hosts_file)))

        with Transaction() as transaction:
            self.shutdown(transaction=transaction)
            self.hypervisor.redefine_vm(self, new_fqdn=new_hostname)

            self.dataset_obj.commit()

            self.start(transaction=transaction)

    def prepare_vm(self):
        """Prepare the rootfs for a VM

        VM storage must be mounted on the hypervisor.
        """
        fd = BytesIO()
        fd.write(self.fqdn.encode())
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
        fd = BytesIO()
        with self.hypervisor.fabric_settings(
            cd(self.hypervisor.vm_mount_path(self))
        ):
            get('/etc/resolv.conf', fd)
        self.put('/etc/resolv.conf', fd)

        self.create_ssh_keys()

    def create_ssh_keys(self):
        # If we wouldn't do remove those, ssh-keygen would ask us confirm
        # overwrite.
        self.run('rm -f /etc/ssh/ssh_host_*_key*')

        self.dataset_obj['sshfp'] = set()
        key_types = [(1, 'rsa'), (3, 'ecdsa')]
        if self.dataset_obj['os'] != 'wheezy':
            key_types.append((4, 'ed25519'))
        fp_types = [(1, sha1), (2, sha256)]

        # This will also create the public key files.
        for key_id, key_type in key_types:
            self.run(
                'ssh-keygen -q -t {0} -N "" '
                '-f /etc/ssh/ssh_host_{0}_key'
                .format(key_type)
            )

            fd = BytesIO()
            self.get('/etc/ssh/ssh_host_{0}_key.pub'.format(key_type), fd)
            pub_key = b64decode(fd.getvalue().split(None, 2)[1])
            for fp_id, fp_type in fp_types:
                self.dataset_obj['sshfp'].add('{} {} {}'.format(
                    key_id, fp_id, fp_type(pub_key).hexdigest()
                ))

    def run_puppet(self, clear_cert=False, debug=False):
        """Runs Puppet in chroot on the hypervisor."""

        if clear_cert:
            with settings(
                host_string=self.dataset_obj['puppet_ca'],
                user='root',
                warn_only=True,
            ):
                run(
                    '/usr/bin/puppet cert clean {}'.format(self.fqdn),
                    shell=False,
                )

        self.block_autostart()

        puppet_command = (
            '( /opt/puppetlabs/puppet/bin/puppet agent '
            '--detailed-exitcodes '
            '--fqdn={} --server={} --ca_server={} '
            '--no-report --waitforcert=60 --onetime --no-daemonize '
            '--skip_tags=chroot_unsafe --verbose{} ) ;'
            '[ $? -eq 2 ]'
            .format(
                self.fqdn,
                self.dataset_obj['puppet_master'],
                self.dataset_obj['puppet_ca'],
                ' --debug' if debug else '',
            )
        )

        try:
            self.run(puppet_command)
        except RemoteCommandError as e:
            raise VMError('Initial puppetrun failed') from e

        self.unblock_autostart()

    def block_autostart(self):
        fd = BytesIO()
        fd.write(b'#!/bin/sh\nexit 101\n')
        self.put('/usr/sbin/policy-rc.d', fd, '0755')

    def unblock_autostart(self):
        self.run('rm /usr/sbin/policy-rc.d')

    def copy_postboot_script(self, script):
        self.put('/buildvm-postboot', script, '0755')

    def prepare_offline_migration(self, no_shutdown, transaction):
        self.set_state('maintenance', transaction=transaction)
        if self.is_running():
            if no_shutdown:
                log.info('Please shut down the VM manually now')
                self.wait_for_running(running=False, timeout=86400)
            else:
                self.shutdown(
                    check_vm_up_on_transaction=False,
                    transaction=transaction,
                )