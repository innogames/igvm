import os
import math

from fabric.api import run, cd, settings, abort, put

from adminapi.utils import IP
from adminapi import api

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
        with settings(warn_only=True):
            run('rm etc/ssh/ssh_host_rsa_key')
            run('rm etc/ssh/ssh_host_dsa_key')
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

def _get_subnet(ip, ranges):
    try:
        return [r for r in ranges if r['belongs_to']][0]
    except IndexError:
        return False

def _get_uppernet(ip, ranges, segment=None):
    try:
        if segment:
            return [r for r in ranges if r['belongs_to'] is None and
                    r['segment'] == segment][0]
        else:
            return [r for r in ranges if r['belongs_to'] is None][0]
    except IndexError:
        return False

def _calc_netmask(iprange):
    host_bits = int(math.ceil(math.log(iprange['max'] - iprange['min'], 2)))
    return IP(-1 << host_bits)

def create_interfaces(primary_ip, additional_ips, target_dir):
    ip_api = api.get('ip')

    ip_info = {}
    ip_info[primary_ip] = {
        'gateway': None,
        'ip': primary_ip
    }
    for ip in additional_ips:
        ip_info[ip] = {
            'gateway': None,
            'ip': ip
        }
    
    gateway_found = False
    primary_ranges = ip_api.get_matching_ranges(primary_ip)
    if primary_ip.is_public():
        net = _get_uppernet(primary_ip, primary_ranges)
        if net:
            gateway_found = True
            ip_info[ip]['gateway'] = IP(net['gateway'])
            ip_info[ip]['netmask'] = _calc_netmask(net)
        else:
            abort('No network found for IP {0}'.format(primary_ip))

    for ip in additional_ips:
        ranges = ip_api.get_matching_ranges(ip)
        if ip.is_public():
            net = _get_uppernet(ip, ranges)
            if net:
                if not gateway_found:
                    gateway_found = True
                    ip_info[ip]['gateway'] = IP(net['gateway'])
                ip_info[ip]['netmask'] = _calc_netmask(net)
            else:
                abort('No network found for IP {0}'.format(ip))
        else:
            pass

    routes = []
    if primary_ip.is_private():
        subnet = _get_subnet(primary_ip, primary_ranges)
        if not subnet:
            abort('No network found for IP {0}'.format(primary_ip))

        uppernet = _get_uppernet(primary_ip, primary_ranges, subnet['segment'])
        if not uppernet:
            abort('No upper network found for IP {0}'.format(primary_ip))

        if not gateway_found:
            ip_info[primary_ip]['gateway'] = IP(subnet['gateway'])
        
        netmask = _calc_netmask(uppernet)
        ip_info[primary_ip]['netmask'] = netmask 

        # Route to other segments
        routes.append({
            'ip': '10.0.0.0',
            'netmask': '255.0.0.0',
            'gw': IP(uppernet['gateway'])
        })

        

    iface_primary_ip = ip_info[primary_ip]
    iface_additional_ips = [ip_info[ip] for ip in additional_ips]

    with cd(target_dir):
        run('mkdir -p etc/network')
        upload_template('etc/network/interfaces', 'etc/network/interfaces', {
            'iface_primary_ip': iface_primary_ip,
            'iface_additional_ips': iface_additional_ips
        })

        if routes:
            upload_template('etc/network/routes', 'etc/network/routes', {
                'routes': routes
            })

def prepare_vm(target_dir, server, mailname, dns_servers, swap_size):
    set_hostname(target_dir, server['hostname'])
    create_ssh_keys(target_dir)
    create_resolvconf(target_dir, dns_servers)
    create_hosts(target_dir)
    create_interfaces(server['intern_ip'], server['additional_ips'], target_dir)
    set_mailname(target_dir, mailname)
    
    swap_path = os.path.join(target_dir, 'swap')
    generate_swap(swap_path, swap_size)

    create_fstab(target_dir)
    create_inittab(target_dir)
    create_authorized_keys(target_dir)

def copy_postboot_script(target_dir, script):
    with cd(target_dir):
        put(script, 'buildvm-postboot', mode=755)
