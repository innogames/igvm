from fabric.api import env, execute, run, prompt

from buildvm.utils import raise_failure, fail_gracefully
from buildvm.utils.units import convert_size
from buildvm.utils.resources import get_meminfo, get_cpuinfo
from buildvm.utils.storage import prepare_storage, umount_temp, remove_temp
from buildvm.utils.image import download_image, extract_image
from buildvm.utils.preparevm import prepare_vm, copy_postboot_script
from buildvm.utils.hypervisor import (create_definition, get_hypervisor,
        start_machine)
from buildvm.utils.portping import wait_until
from buildvm.signals import send_signal


run = fail_gracefully(run)


def check_config(config):
    send_signal('prefill_config', config)

    if 'host' not in config:
        config['host'] = prompt('Hostname for dom0:',
                validate=r'^[a-z][a-z0-9_-]+')

    if 'mem' not in config:
        config['mem'] = int(prompt('Memory (in MiB):', validate=r'^\d+$'))

    if 'num_cpu' not in config:
        config['num_cpu'] = int(prompt('Number of CPUs:', validate='^\d+$'))

    if 'disk_size' not in config:
        config['disk_size'] = int(prompt('Disk size (in MiB):',
                validate=r'^\d+$'))

    if 'image' not in config:
        config['image'] = prompt('Image:', validate='^[\w_-]+\.tar\.gz$')

    send_signal('postfill_config', config)


def setup(config):
    check_config(config)

    env.use_ssh_config = True
    env.always_use_pty = False
    env.shell = '/bin/bash -c'
    env.hosts = [config['host']]
    execute(setup_hardware, config)
    env.hosts = [config['hostname']]
    execute(setup_guest, config)

def setup_hardware(config, boot=True):
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

    if 'postboot_script' in config:
        copy_postboot_script(mount_path, config['postboot_script'])

    umount_temp(device)
    remove_temp(mount_path)

    server = config['server']
    hypervisor = get_hypervisor()
    create_definition(server['hostname'], config['num_cpu'], config['mem'],
            config['mem'], device, hypervisor)

    if not boot:
        return

    start_machine(server['hostname'], hypervisor)

    host_up = wait_until(server['intern_ip'].as_ip(),
            waitmsg='Waiting for guest to boot')

    if not host_up:
        raise_failure(Exception('Guest did not boot.'))


def setup_guest(config):
    if 'postboot_script' in config:
        run('/buildvm-postboot')
        run('rm -f /buildvm-postboot')


if __name__ == '__main__':
    setup(None)
