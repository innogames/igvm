from base64 import b64decode
from hashlib import sha256
import logging
import os
from StringIO import StringIO

from fabric.api import run, cd, get, put, settings

from igvm.settings import DEFAULT_SWAP_SIZE
from igvm.utils.sshkeys import create_authorized_keys
from igvm.utils.template import upload_template
from igvm.utils import cmd


log = logging.getLogger(__name__)


def _create_ssh_keys(os):

    # If we wouldn't do remove those, ssh-keygen would ask us confirm
    # overwrite.
    run('rm -f etc/ssh/ssh_host_*_key*')

    if os == 'wheezy':
        key_types = ('dsa', 'rsa', 'ecdsa')
    else:
        key_types = ('dsa', 'rsa', 'ecdsa', 'ed25519')

    # This will also create the public key files.
    for key_type in key_types:
        run(
            # Use ssh-keygen from chroot in case hypervisor OS is too old
            'chroot . ssh-keygen -q -t {0} -N "" -f etc/ssh/ssh_host_{0}_key'
            .format(key_type)
        )


def _get_ssh_public_key(key_type):
    fd = StringIO()
    get('etc/ssh/ssh_host_{0}_key.pub'.format(key_type), fd)
    key_split = fd.getvalue().split()

    return key_split[1]


def _generate_swap(swap_path, size_MiB):
    run(cmd('dd if=/dev/zero of={0} bs=1M count={1}', swap_path, size_MiB))
    run(cmd('/sbin/mkswap {0}', swap_path))
    run(cmd('/bin/chmod 0600 {0}', swap_path))


def _create_interfaces(network_config):
    run('mkdir -p etc/network')
    upload_template('etc/network/interfaces', 'etc/network/interfaces', {
        'network_config': network_config,
    })


def block_autostart(hypervisor, vm):
    target_dir = hypervisor.vm_mount_path(vm)
    with cd(target_dir):
        hypervisor.run('echo "#!/bin/sh" >> usr/sbin/policy-rc.d')
        hypervisor.run('echo "exit 101"  >> usr/sbin/policy-rc.d')
        hypervisor.run('chmod +x usr/sbin/policy-rc.d')


def unblock_autostart(hypervisor, vm):
    target_dir = hypervisor.vm_mount_path(vm)
    with cd(target_dir):
        hypervisor.run('rm usr/sbin/policy-rc.d')


def prepare_vm(hypervisor, vm):
    """Prepare the rootfs for a VM

    VM storage must be mounted on the hypervisor.
    """
    target_dir = hypervisor.vm_mount_path(vm)
    with hypervisor.fabric_settings(cd(target_dir)):
        run(cmd('echo {0} > etc/hostname', vm.fqdn))
        run(cmd('echo {0} > etc/mailname', vm.fqdn))

        _create_interfaces(vm.network_config)
        _create_ssh_keys(vm.server_obj['os'])
        vm.server_obj['ssh_pubkey'] = _get_ssh_public_key('rsa')
        vm.server_obj['ssh_ecdsa_fp'] = sha256(b64decode(
            _get_ssh_public_key('ecdsa')
        )).hexdigest()

        upload_template('etc/fstab', 'etc/fstab', {
            'blk_dev': hypervisor.vm_block_device_name(),
            'type': 'xfs',
            'mount_options': 'defaults'
        })
        upload_template('etc/hosts', 'etc/hosts')
        upload_template('etc/inittab', 'etc/inittab')
        run(cmd('cp /etc/resolv.conf etc/resolv.conf'))

        swap_path = os.path.join(target_dir, 'swap')
        _generate_swap(swap_path, DEFAULT_SWAP_SIZE)

        create_authorized_keys(target_dir)


def copy_postboot_script(hypervisor, vm, script):
    target_dir = hypervisor.vm_mount_path(vm)
    with hypervisor.fabric_settings(cd(target_dir)):
        put(script, 'buildvm-postboot', mode=755)


def run_puppet(hypervisor, vm, clear_cert, tx):
    """Runs Puppet in chroot on the hypervisor."""
    target_dir = hypervisor.vm_mount_path(vm)
    block_autostart(hypervisor, vm)

    if clear_cert:
        with settings(host_string=vm.server_obj['puppet_ca'], warn_only=True):
            run(cmd('/usr/bin/puppet cert clean {}', vm.fqdn))

    with cd(target_dir):
        if tx:
            tx.on_rollback(
                'Kill puppet',
                hypervisor.run,
                'pkill -9 -f "/usr/bin/puppet agent -v --fqdn={}"'
                .format(vm.fqdn)
            )
        hypervisor.run(
            'chroot . /usr/bin/puppet agent -v --fqdn={}'
            ' --server {} --ca_server {} --no-report'
            ' --waitforcert=60 --onetime --no-daemonize'
            ' --tags=network,puppet,check_logfiles'
            ' --skip_tags=nrpe,chroot_unsafe'
            .format(
                vm.fqdn,
                vm.server_obj['puppet_master'],
                vm.server_obj['puppet_ca'],
            )
        )

    unblock_autostart(hypervisor, vm)
