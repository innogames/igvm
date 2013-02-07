from fabric.api import env, execute

import adminapi
from adminapi.utils import IP

from buildvm.utils import raise_failure
from buildvm.utils.units import convert_size
from buildvm.utils.resources import get_meminfo, get_cpuinfo
from buildvm.utils.storage import prepare_storage, umount_temp
from buildvm.utils.image import download_image, extract_image
from buildvm.utils.preparevm import prepare_vm
from buildvm.utils.hypervisor import create_sxp


def setup(config):
    hostname = 'test-buildvm'
    config = {
        'hostname': hostname,
        'mem': 256,
        'num_cpu': 1,
        'disk_size': 4096,
        'mailname': 'test-buildvm' + '.ig.local',
        'dns_servers': ['10.0.0.102', '10.0.0.85', '10.0.0.83'],
        'swap_size': 1024,
        'image': 'wheezy-base.tar.gz',
        'server': {
            'hostname': hostname,
            'intern_ip': IP('10.4.0.15'),
            #'additional_ips': set([IP('212.48.98.12')])
            'additional_ips': set()
        }
    }

    env.use_ssh_config = True
    env.always_use_pty = False
    env.shell = '/bin/bash -c'
    env.hosts = ['af05db005']
    execute(setup_hardware, config)
    env.hosts = [config['hostname']]
    execute(setup_vm, config)

def setup_hardware(config):
    meminfo = get_meminfo()
    cpuinfo = get_cpuinfo()

    mem_free = meminfo['MemFree'] + meminfo['Buffers'] + meminfo['Cached']
    mem_free = convert_size(mem_free, 'B', 'M')
    if config['mem'] > mem_free:
        mem_missing = config['mem'] - mem_free
        raise_failure(Exception('Not enough free memory. Missing {0} MiB',
                mem_missing))

    num_cpus = len(cpuinfo)
    if config['num_cpu'] > num_cpus:
        raise_failure(Exception('Not enough CPUs.'))

    device, mount_path = prepare_storage(config['hostname'],
            config['disk_size'])

    download_image(config['image'])
    extract_image(config['image'], mount_path)

    prepare_vm(mount_path,
            server=config['server'],
            mailname=config['mailname'],
            dns_servers=config['dns_servers'],
            swap_size=config['swap_size'])

    umount_temp(device)

    server = config['server']
    create_sxp(server['hostname'], config['num_cpu'], config['mem'],
            config['mem'], device)


def setup_vm(config):
    pass

if __name__ == '__main__':
    setup(None)
