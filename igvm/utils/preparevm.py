import os

from fabric.api import run, cd, put, settings

from managevm.utils.sshkeys import create_authorized_keys
from managevm.utils.template import upload_template
from managevm.utils import cmd


PUPPET_CA_MASTERS = (
    # Puppet 3
    'master.puppet.ig.local',
    # Puppet 4
    'ca.puppet.ig.local',
)

def set_hostname(target_dir, hostname):
    with cd(target_dir):
        run(cmd('echo {0} > etc/hostname', hostname))

def create_ssh_keys(target_dir, ssh_keytypes):
    with cd(target_dir):
        for typ in ssh_keytypes:
            run('rm -f etc/ssh/ssh_host_{0}_key'.format(typ))
            run('ssh-keygen -q -t {0} -N "" -f etc/ssh/ssh_host_{0}_key'.format(typ))

def create_resolvconf(target_dir, dns_servers):
    with cd(target_dir):
        upload_template('etc/resolv.conf', 'etc/resolv.conf', {
            'dns_servers': dns_servers
        })

def create_hosts(target_dir):
    with cd(target_dir):
        upload_template('etc/hosts', 'etc/hosts')

def create_inittab(target_dir):
    with cd(target_dir):
        upload_template('etc/inittab', 'etc/inittab')

def set_mailname(target_dir, mailname):
    with cd(target_dir):
        run(cmd('echo {0} > etc/mailname', mailname))

def generate_swap(swap_path, size_MiB):
    run(cmd('dd if=/dev/zero of={0} bs=1M count={1}', swap_path, size_MiB))
    run(cmd('/sbin/mkswap {0}', swap_path))

def create_fstab(target_dir, blk_dev):
    with cd(target_dir):
        upload_template('etc/fstab', 'etc/fstab', {
            'blk_dev' : blk_dev,
            'type': 'xfs',
            'mount_options': 'defaults'
        })

def create_interfaces(network_config, target_dir):

    with cd(target_dir):
        run('mkdir -p etc/network')
        upload_template('etc/network/interfaces', 'etc/network/interfaces', {
            'network_config': network_config,
        })

def block_autostart(hv, vm):
    target_dir = hv.vm_mount_path(vm)
    with cd(target_dir):
        hv.run('echo "#!/bin/sh" >> usr/sbin/policy-rc.d' )
        hv.run('echo "exit 101"  >> usr/sbin/policy-rc.d' )
        hv.run('chmod +x usr/sbin/policy-rc.d' )

def unblock_autostart(hv, vm):
    target_dir = hv.vm_mount_path(vm)
    with cd(target_dir):
        hv.run('rm usr/sbin/policy-rc.d' )


def prepare_vm(target_dir, server, mailname, dns_servers, network_config,
               swap_size, blk_dev, ssh_keytypes):
    set_hostname(target_dir, server['hostname'])
    create_ssh_keys(target_dir, ssh_keytypes)
    create_resolvconf(target_dir, dns_servers)
    create_hosts(target_dir)
    create_interfaces(network_config, target_dir)
    set_mailname(target_dir, mailname)

    swap_path = os.path.join(target_dir, 'swap')
    generate_swap(swap_path, swap_size)

    create_fstab(target_dir, blk_dev)
    create_inittab(target_dir)
    create_authorized_keys(target_dir)

def copy_postboot_script(target_dir, script):
    with cd(target_dir):
        put(script, 'buildvm-postboot', mode=755)

def run_puppet(hv, vm, clear_cert):
    """Runs Puppet in chroot on the hypervisor."""
    target_dir = hv.vm_mount_path(vm)
    block_autostart(hv, vm)

    if clear_cert:
        for puppet_master in PUPPET_CA_MASTERS:
            with settings(host_string=puppet_master, warn_only=True):
                run(cmd(
                    '/usr/bin/puppet cert clean {0}.ig.local'
                    ' || echo "No cert for Host found"',
                    vm.hostname,
                ), warn_only=True)

    with cd(target_dir):
        hv.run(
            'chroot . /usr/bin/puppet agent -v --fqdn={}.ig.local'
            ' --waitforcert 60 --onetime --no-daemonize'
            ' --tags network,internal_routes'
            .format(vm.hostname)
        )

    unblock_autostart(hv, vm)
