from __future__ import division

import os
import re
from contextlib import nested

from fabric.utils import warn
from fabric.api import run, settings, hide, puts, prompt
from fabric.contrib.console import confirm

from managevm.utils.units import convert_size
from managevm.utils import cmd, fail_gracefully, raise_failure

run = fail_gracefully(run)

class StorageError(Exception):
    pass

def get_volume_groups():
    with settings(warn_only=True):
        lvminfo = run('vgdisplay -c')

    if lvminfo.failed:
        warn("No LVM found")
        raise_failure(StorageError("No LVM found"))


    vgroups = []
    for line in lvminfo.splitlines():
        parts = line.strip().split(':')
        if len(parts) != 17:
            print("Badly formatted vgdisplay output: {0}".format(line))
            continue
        volume_group = parts[0]
        size_KiB    = int(parts[11])
        size_MiB    = int(size_KiB / 1024)
        pe_size_KiB = int(parts[12])
        free_exts   = int(parts[15])
        free_MiB    = int(free_exts * pe_size_KiB / 1024)

        vgroups.append({
            'name': volume_group,
            'size_total_MiB': size_MiB,
            'size_free_MiB': free_MiB,
            'pe_size_KiB': pe_size_KiB,
        })

    return vgroups

def get_logical_volumes():
    vgs = get_volume_groups()

    with settings(warn_only=True):
        lvminfo = run('lvdisplay -c')

    if lvminfo.failed:
        warn("No LVM found")
        raise_failure(StorageError("No LVM found"))

    lvolumes = []
    for line in lvminfo.splitlines():
        parts = line.strip().split(':')
        if len(parts) != 13:
            print("Badly formatted lvdisplay output: {0}".format(line))
            continue
        logical_volume = parts[0]
        volume_group   = parts[1]

        for volume_group_test in vgs:
            if volume_group_test['name'] == volume_group:
                pe_size_KiB = volume_group_test['pe_size_KiB']

        size_KiB = int(parts[7]) * pe_size_KiB
        size_MiB = int(size_KiB / 1024)

        lvolumes.append({
            'name': logical_volume,
            'size_MiB': size_MiB,
        })

    return lvolumes


def create_logical_volume(volume_group, name, size_GiB):
    lvs = [lv.strip().split(':') for lv in run('lvdisplay -c').splitlines()]
    lvs = [lv for lv in lvs if lv[1] == volume_group]
    volume = os.path.join('/dev', volume_group, name)
    if volume in [lv[0] for lv in lvs]:
        rem = confirm('Logical volume already exists. Should I remove it?')
        if rem:
            puts('Please remove the VM for this volume if it exists.')
            prompt('Press any key to continue.')
            with settings(warn_only=True):
                run(cmd('umount {0}', volume))
            run(cmd('lvremove -f {0}', volume))

    run(cmd('lvcreate -L {0}G -n {1} {2}', size_GiB, name, volume_group))
    return volume

def format_device(device):
    with settings(warn_only=True):
        status = run(cmd('mkfs.xfs {0}', device))
    if status.failed:
        if confirm('Force mkfs.xfs?'):
            run(cmd('mkfs.xfs -f {0}', device))

def mount_temp(device, suffix=''):
    mount_dir = run(cmd('mktemp -d --suffix {0}', suffix))
    run(cmd('mount {0} {1}', device, mount_dir))
    return mount_dir

def umount_temp(device_or_path):
    run(cmd('umount {0}', device_or_path))

def remove_temp(mount_path):
    run(cmd('rm -rf {0}', mount_path))

def get_vm_block_dev(hypervisor):
    if hypervisor == 'xen':
        return 'xvda1'
    elif hypervisor == 'kvm':
        return 'vda'
    else:
        raise_failure(StorageError("VM block device name unknown for hypervisor {0}".format(hypervisor)))

def get_storage_type():
    with nested(settings(warn_only=True), hide('everything')):
        result = run('which santool')
    return 'san' if not result.failed else 'lvm'

def get_san_arrays():
    saninfo = run('santool --show free')

    arrays = []
    array = None
    for line in saninfo.splitlines():
        line = line.strip()
        if line.startswith('Array:'):
            if array:
                arrays.append(array)
            match = re.match(r'Array:\s+(\w+)\s+\((\d+)\)', line)
            if match:
                array = {'id': match.group(1),
                         'no': int(match.group(2))}
            else:
                array = None
        elif line.startswith('free luns:') and array:
            match = re.match('free luns:\s+(\d+) of (\d+)\s+\d+% free', line)
            if match:
                array['num_free'] = int(match.group(1))
                array['num_total'] = int(match.group(2))
    if array:
        arrays.append(array)

    return arrays

def choose_array(arrays):
    return max(arrays, key=lambda x: x['num_free'] / x['num_total'])

def create_san_raid(name, array):
    run(cmd('santool --build-raid -u {0} --array-number {1}', name, array))
    return os.path.join('/dev', 'san', 'raid', name)

def create_storage(hostname, disk_size_gib):
    storage_type = get_storage_type()
    if storage_type == 'san':
        san_arrays = get_san_arrays()
        array = choose_array(san_arrays)
        device = create_san_raid(hostname, array['no'])
    else:
        volume_groups = get_volume_groups()
        if not volume_groups:
            raise_failure(StorageError('No volume groups found'))
        volume_group = volume_groups[0]
        volume = volume_group['name']
        if convert_size(volume_group['size_free_MiB'], 'M', 'G') < disk_size_gib:
            raise_failure(StorageError('No enough free space'))
        device = create_logical_volume(volume, hostname, disk_size_gib)

    return device

def mount_storage(device, hostname):
    format_device(device)
    mount_path = mount_temp(device, suffix='-' + hostname)
    return mount_path

def netcat_to_device(device):
    port = 4242
    # Using DD lowers load on device with big enough Block Size
    run('nohup /bin/nc.traditional -l -p {0} | dd of={1} obs=1048576 &'.format(port, device))
    return port

def device_to_netcat(device, size, host, port):
    run('dd if={0} ibs=1048576 | pv -f -s {1} | /bin/nc.traditional -q 1 {2} {3}'.format(device, size, host, port))
