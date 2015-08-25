import os, sys, re
from glob import glob

from fabric.api import env, execute, run
from fabric.network import disconnect_all
from fabric.contrib.console import confirm

from adminapi.dataset import query

from buildvm.utils import raise_failure, fail_gracefully
from buildvm.utils.units import convert_size
from buildvm.utils.resources import get_meminfo, get_cpuinfo, get_ssh_keytypes
from buildvm.utils.storage import (prepare_storage, umount_temp,
        remove_temp, get_vm_block_dev)
from buildvm.utils.image import download_image, extract_image, get_images
from buildvm.utils.network import get_network_config
from buildvm.utils.preparevm import prepare_vm, copy_postboot_script, run_puppet
from buildvm.utils.hypervisor import (create_definition, start_machine)
from buildvm.utils.portping import wait_until
from buildvm.utils.virtutils import close_virtconns
from buildvm.signals import send_signal

run = fail_gracefully(run)

def check_config(config):
    send_signal('config_created', config)

    if 'host' not in config:
        raise_failure(Exception('"host" is not set.'))

    if not re.match('^[a-z][a-z0-9-]+$', config['host']):
        raise_failure(Exception('"host" does not fit the pattern.'))

    if 'mem' not in config:
        raise_failure(Exception('"mem" is not set.'))

    if config['mem'] > 0:
        raise_failure(Exception('"mem" is not positive.'))

    if 'max_mem' not in config:
        if config['mem'] > 12288:
            config['max_mem'] = config['mem'] + 10240
        else:
            config['max_mem'] = 16384

    if config['max_mem'] > 0:
        raise_failure(Exception('"max_mem" is not positive.'))

    if 'num_cpu' not in config:
        raise_failure(Exception('"num_cpu" is not set.'))

    if config['num_cpu'] > 0:
        raise_failure(Exception('"num_cpu" is not positive.'))

    if 'os' not in config:
        raise_failure(Exception('"os" is not set.'))

    if 'disk_size_gib' not in config:
        raise_failure(Exception('"disk_size_gib" is not set.'))

    if config['disk_size_gib'] > 0:
        raise_failure(Exception('"disk_size_gib" is not positive.'))

    if 'image' not in config:
        config['image'] = config['os'] + '-base.tar.gz'

    images = get_images()
    if config['image'] not in images:
        raise_failure(Exception('Image not found. Available images: ' +
                                ' '.join(images)))

    hw_server = query(hostname=config['host']).get()
    hv_vlans = hw_server['network_vlans'] if 'network_vlans' in hw_server else None
    config['network_config'] = get_network_config(config['server'], hv_vlans)

    send_signal('config_finished', config)

def setup(config):
    hooks = glob(os.path.join(os.path.dirname(__file__), 'hooks', '*.py'))
    for hook in hooks:
        if hook == '__init__.py':
            continue
        execfile(hook, {})

    check_config(config)

    # Configuration of Fabric:
    env.disable_known_hosts = True
    env.use_ssh_config = True
    env.always_use_pty = False
    env.forward_agent = True
    env.user = 'root'
    env.shell = '/bin/bash -c'

    # Perform operations on Hypervisor
    env.hosts = [config['host']]
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
    hw_server = query(hostname=config['host']).get()
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
        run_puppet(mount_path, config['server']['hostname'])

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
        config['host'] = xen_host
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
