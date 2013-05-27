import os

from fabric.api import run, cd, put


from buildvm.utils.sshkeys import create_authorized_keys
from buildvm.utils.template import upload_template
from buildvm.utils import fail_gracefully, cmd

run = fail_gracefully(run)
put = fail_gracefully(put)

def set_hostname(target_dir, hostname):
    with cd(target_dir):
        run(cmd('echo {0} > etc/hostname', hostname))

def create_ssh_keys(target_dir):
    with cd(target_dir):
        run('rm -f etc/ssh/ssh_host_rsa_key')
        run('rm -f etc/ssh/ssh_host_dsa_key')
        run('ssh-keygen -q -t rsa -N "" -f etc/ssh/ssh_host_rsa_key')
        run('ssh-keygen -q -t dsa -N "" -f etc/ssh/ssh_host_dsa_key')

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

def generate_swap(swap_path, size_mb):
    run(cmd('dd if=/dev/zero of={0} bs=1M count={1}', swap_path, size_mb))
    run(cmd('/sbin/mkswap {0}', swap_path))

def create_fstab(target_dir):
    with cd(target_dir):
        upload_template('etc/fstab', 'etc/fstab', {
            'type': 'xfs',
            'mount_options': 'defaults'
        })

def create_interfaces(primary_ip, additional_ips, network_config, target_dir):
    routes = network_config['routes']
    ip_info = network_config['ip_info']
    loadbalancer = network_config['loadbalancer']

    iface_primary_ip = ip_info[primary_ip]
    iface_additional_ips = [ip_info[ip] for ip in additional_ips]

    with cd(target_dir):
        run('mkdir -p etc/network')
        upload_template('etc/network/interfaces', 'etc/network/interfaces', {
            'iface_primary_ip': iface_primary_ip,
            'iface_additional_ips': iface_additional_ips,
            'setup_loadbalancer': loadbalancer
        })
        if loadbalancer:
            upload_template('etc/network/lb', 'etc/network/lb', {
                'loadbalancer': loadbalancer
            })

        if routes:
            upload_template('etc/network/routes', 'etc/network/routes', {
                'routes': routes
            })

def prepare_vm(target_dir, server, mailname, dns_servers, network_config,
               swap_size):
    set_hostname(target_dir, server['hostname'])
    create_ssh_keys(target_dir)
    create_resolvconf(target_dir, dns_servers)
    create_hosts(target_dir)
    create_interfaces(server['intern_ip'], server['additional_ips'],
            network_config, target_dir)
    set_mailname(target_dir, mailname)
    
    swap_path = os.path.join(target_dir, 'swap')
    generate_swap(swap_path, swap_size)

    create_fstab(target_dir)
    create_inittab(target_dir)
    create_authorized_keys(target_dir)

def copy_postboot_script(target_dir, script):
    with cd(target_dir):
        put(script, 'buildvm-postboot', mode=755)
