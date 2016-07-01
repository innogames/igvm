import httplib
import logging
import os
from StringIO import StringIO
import socket
import urllib

from fabric.api import run, cd, get, put, settings

from igvm.exceptions import IGVMError
from igvm.settings import (
    DEFAULT_DNS_SERVERS,
    DEFAULT_SWAP_SIZE,
    PUPPET_CA_MASTERS,
)
from igvm.utils.sshkeys import create_authorized_keys
from igvm.utils.template import upload_template
from igvm.utils import cmd


log = logging.getLogger(__name__)


def _create_ssh_keys():

    # If we wouldn't do remove those, ssh-keygen would ask us confirm
    # overwrite.
    run('rm -f etc/ssh/ssh_host_*_key*')

    # This will also create the public key files.
    for key_type in ('dsa', 'rsa', 'ecdsa', 'ed25519'):
        run(
            # Use ssh-keygen from chroot in case HV OS is too old
            'chroot . ssh-keygen -q -t {0} -N "" -f etc/ssh/ssh_host_{0}_key'
            .format(key_type)
        )


def _get_ssh_public_key(key_type):
    fd = StringIO()
    get('etc/ssh/ssh_host_{0}_key.pub'.format(key_type), fd)
    key_split = fd.getvalue().split()

    assert key_split[0] == 'ssh-' + key_type
    return key_split[1]


def _generate_swap(swap_path, size_MiB):
    run(cmd('dd if=/dev/zero of={0} bs=1M count={1}', swap_path, size_MiB))
    run(cmd('/sbin/mkswap {0}', swap_path))


def _create_interfaces(network_config):
    run('mkdir -p etc/network')
    upload_template('etc/network/interfaces', 'etc/network/interfaces', {
        'network_config': network_config,
    })


def block_autostart(hv, vm):
    target_dir = hv.vm_mount_path(vm)
    with cd(target_dir):
        hv.run('echo "#!/bin/sh" >> usr/sbin/policy-rc.d')
        hv.run('echo "exit 101"  >> usr/sbin/policy-rc.d')
        hv.run('chmod +x usr/sbin/policy-rc.d')


def unblock_autostart(hv, vm):
    target_dir = hv.vm_mount_path(vm)
    with cd(target_dir):
        hv.run('rm usr/sbin/policy-rc.d')


def prepare_vm(hv, vm):
    """Prepares the rootfs for a VM. VM storage must be mounted on the HV."""
    target_dir = hv.vm_mount_path(vm)
    with hv.fabric_settings(cd(target_dir)):
        run(cmd('echo {0} > etc/hostname', vm.hostname))
        run(cmd('echo {0} > etc/mailname', vm.fqdn))

        _create_interfaces(vm.network_config)
        _create_ssh_keys()
        vm.admintool['ssh_pubkey'] = _get_ssh_public_key('rsa')

        upload_template('etc/fstab', 'etc/fstab', {
            'blk_dev': hv.vm_block_device_name(),
            'type': 'xfs',
            'mount_options': 'defaults'
        })
        upload_template('etc/hosts', 'etc/hosts')
        upload_template('etc/inittab', 'etc/inittab')
        upload_template('etc/resolv.conf', 'etc/resolv.conf', {
            'dns_servers': DEFAULT_DNS_SERVERS
        })

        swap_path = os.path.join(target_dir, 'swap')
        _generate_swap(swap_path, DEFAULT_SWAP_SIZE)

        create_authorized_keys(target_dir)


def copy_postboot_script(hv, vm, script):
    target_dir = hv.vm_mount_path(vm)
    with hv.fabric_settings(cd(target_dir)):
        put(script, 'buildvm-postboot', mode=755)


def _clear_cert_controller(hostname, puppet_master, token):
    try:
        conn = httplib.HTTPConnection('{}:9000'.format(puppet_master))
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "Accept": "text/plain",
        }
        conn.request("POST", "/", urllib.urlencode({
            'token': token,
            'hostname': hostname,
            'command': 'clear',
        }), headers)
        response = conn.getresponse()
        if response.status != 200:
            raise IGVMError(response.read().strip())
    except (socket.error, httplib.HTTPException) as e:
        raise IGVMError(e)


def run_puppet(hv, vm, clear_cert, tx):
    """Runs Puppet in chroot on the hypervisor."""
    target_dir = hv.vm_mount_path(vm)
    block_autostart(hv, vm)

    if clear_cert:
        # Use puppet-controller, if possible.
        puppet_masters = set(PUPPET_CA_MASTERS[:])
        controller_token = os.environ.get('PUPPET_CONTROLLER_TOKEN')
        if controller_token:
            for puppet_master in PUPPET_CA_MASTERS:
                try:
                    _clear_cert_controller(
                        vm.hostname,
                        puppet_master,
                        controller_token,
                    )
                    puppet_masters.remove(puppet_master)
                    log.info(
                        'Cleared Puppet cert of {} on {}'
                        .format(vm.hostname, puppet_master)
                    )
                except IGVMError as e:
                    log.info(
                        'Failed to clear Puppet cert of {} on {}: {}'
                        .format(vm.hostname, puppet_master, e)
                    )

        # Use SSH for all remaining servers
        for puppet_master in puppet_masters:
            with settings(host_string=puppet_master, warn_only=True):
                run(cmd(
                    '/usr/bin/puppet cert clean {0}.ig.local'
                    ' || echo "No cert for Host found"',
                    vm.hostname,
                ), warn_only=True)

    with cd(target_dir):
        if tx:
            tx.on_rollback(
                'Kill puppet',
                hv.run,
                'pkill -9 -f "/usr/bin/puppet agent -v --fqdn={}.ig.local"'
                .format(vm.hostname)
            )
        hv.run(
            'chroot . /usr/bin/puppet agent -v --fqdn={}.ig.local'
            ' --waitforcert 60 --onetime --no-daemonize'
            ' --tags network,internal_routes'
            .format(vm.hostname)
        )

    unblock_autostart(hv, vm)
