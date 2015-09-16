import os, sys, re
from glob import glob

from fabric.api import env, execute, run
from fabric.network import disconnect_all
from fabric.contrib.console import confirm

from adminapi.dataset import query

from managevm.utils import raise_failure, fail_gracefully
from managevm.utils.units import convert_size
from managevm.utils.resources import get_meminfo, get_cpuinfo, get_ssh_keytypes
from managevm.utils.storage import (prepare_storage, umount_temp,
        remove_temp, get_vm_block_dev)
from managevm.utils.image import download_image, extract_image, get_images
from managevm.utils.network import get_network_config
from managevm.utils.preparevm import prepare_vm, copy_postboot_script, run_puppet, block_autostart, unblock_autostart
from managevm.utils.hypervisor import (create_definition, start_machine, check_hv_mem)
from managevm.utils.portping import wait_until
from managevm.utils.virtutils import close_virtconns
from managevm.signals import send_signal

run = fail_gracefully(run)

def buildvm(config):
    hooks = glob(os.path.join(os.path.dirname(__file__), 'hooks', '*.py'))
    for hook in hooks:
        if hook == '__init__.py':
            continue
        execfile(hook, {})

    check_vm_config(config)

    # Configuration of Fabric:
    env.disable_known_hosts = True
    env.use_ssh_config = True
    env.always_use_pty = False
    env.forward_agent = True
    env.user = 'root'
    env.shell = '/bin/bash -c'

    # Perform operations on Hypervisor
    env.hosts = [config['hv_host']]
    execute(setup_hardware, config)

    # Perform operations on Virtual Machine
    env.hosts = [config['hostname']]
    execute(setup_guest, config)

    close_virtconns()
    disconnect_all()

def setup_hardware(config, boot=True):
    send_signal('setup_hardware', config, boot)
    meminfo = get_meminfo()
    cpuinfo = get_cpuinfo()
    hw_server = query(hostname=config['hv_host']).get()
    hypervisor = hw_server.get('hypervisor', "")

    mem_free = meminfo['MemFree'] + meminfo['Buffers'] + meminfo['Cached']
    mem_free = convert_size(mem_free, 'B', 'M')
    if config['mem'] > mem_free:
        mem_missing = config['mem'] - mem_free
        raise_failure(Exception('Not enough free memory. Missing {0} MiB',
                mem_missing))

    num_cpus = len(cpuinfo)
    if config['num_cpu'] > num_cpus:
        raise_failure(Exception('Not enough CPUs.'))

    config['vm_block_dev'] = get_vm_block_dev(hypervisor)

    device, mount_path = prepare_storage(config['hostname'],
            config['disk_size_gib'])

    download_image(config['image'])
    extract_image(config['image'], mount_path, hw_server.get('os', ""))

    send_signal('prepare_vm', config, device, mount_path)
    prepare_vm(mount_path,
            server=config['server'],
            mailname=config['mailname'],
            dns_servers=config['dns_servers'],
            network_config=config['network_config'],
            swap_size=config['swap_size'],
            blk_dev=config['vm_block_dev'],
            ssh_keytypes=get_ssh_keytypes(config['os']))
    send_signal('prepared_vm', config, device, mount_path)

    if config['runpuppet']:
        block_autostart(mount_path)
        run_puppet(mount_path, config['server']['hostname'])
        unblock_autostart(mount_path)

    if 'postboot_script' in config:
        copy_postboot_script(mount_path, config['postboot_script'])

    umount_temp(device)
    remove_temp(mount_path)

    server = config['server']
    hypervisor_extra = {}
    for extra in send_signal('hypervisor_extra', config, hypervisor):
        hypervisor_extra.update(extra)

    create_definition(server['hostname'], config['num_cpu'], config['mem'],
            config['max_mem'], config['network_config']['vlan'],
            device, hypervisor, hypervisor_extra)
    send_signal('defined_vm', config, hypervisor)

    if not boot:
        return

    start_machine(server['hostname'], hypervisor)

    host_up = wait_until(server['intern_ip'].as_ip(),
            waitmsg='Waiting for guest to boot')

    if not host_up:
        raise_failure(Exception('Guest did not boot.'))


def setup_guest(config):
    send_signal('vm_booted', config)
    if 'postboot_script' in config:
        run('/buildvm-postboot')
        run('rm -f /buildvm-postboot')
        send_signal('postboot_executed', config)

def get_config(hostname):
    server = query(hostname=hostname).get()
    config = {
        'server': server,
        'hostname': hostname,
        'swap_size': 1024,
        'mailname': hostname + '.ig.local',
        'dns_servers': ['10.0.0.102', '10.0.0.85', '10.0.0.83'],
    }
    xen_host = server.get('xen_host')
    if xen_host:
        config['hv_host'] = xen_host
    mem = server.get('memory')
    if mem:
        config['mem'] = mem
    num_cpu = server.get('num_cpu')
    if num_cpu:
        config['num_cpu'] = num_cpu
    disk_size_gib = server.get('disk_size_gib')
    if disk_size_gib:
        config['disk_size_gib'] = disk_size_gib
    os = server.get('os')
    if os:
        config['os'] = os

    return config
