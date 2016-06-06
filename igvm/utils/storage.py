from __future__ import division

import logging
import math

from fabric.api import run

from igvm.exceptions import StorageError
from igvm.utils import cmd

log = logging.getLogger(__name__)


def get_logical_volumes(host):
    lvolumes = []
    lvs = host.run(
        'lvs --noheadings -o name,vg_name,lv_size --unit m --nosuffix',
        silent=True
    )
    for lv_line in lvs.splitlines():
        lv_name, vg_name, lv_size = lv_line.split()
        lvolumes.append({
            'path': '/dev/{}/{}'.format(vg_name, lv_name),
            'name': lv_name,
            'vg_name': vg_name,
            'size_MiB': math.ceil(float(lv_size)),
        })
    return lvolumes


def get_vm_volume(hv, vm):
    """Returns the path of the LV belonging to the given VM."""
    for lv in get_logical_volumes(hv):
        if lv['name'] == vm.hostname:
            disk_size = vm.admintool['disk_size_gib']
            if disk_size != int(math.ceil(lv['size_MiB'] / 1024)):
                raise StorageError((
                    "Server disk_size_gib {0} on Serveradmin doesn't "
                    'match the volume size {1} MiB.'
                ).format(disk_size, lv['size_MiB']))

            return lv['path']
    raise StorageError('Unable to find source LV of {}'.format(vm.hostname))


def remove_logical_volume(host, lv):
    host.run(cmd('lvremove -f {0}', lv))


def lvresize(volume, size_gib):
    """Extend the volume, return the new size"""

    run('lvresize {0} -L {1}g'.format(volume, size_gib))


def create_storage(hv, vm):
    # Do not search only for the given LV.
    # `lvs` must generally not fail and give a list of LVs.
    lvs = hv.run(
        'lvs --noheading -o vg_name,name',
        silent=True
    )
    for lv_line in lvs.splitlines():
        vg_name, lv_name = lv_line.split()
        if lv_name == vm.hostname:
            raise StorageError(
                'Logical Volume {}/{} already exists!'
                .format(vg_name, lv_name)
            )
    # Find VG with enough free space
    vgs = hv.run(
        'vgs --noheadings -o vg_name,vg_free --unit g --nosuffix',
        silent=True
    )
    disk_size_gib = vm.admintool['disk_size_gib']
    for vg_line in vgs.splitlines():
            vg_name, vg_size_GiB = vg_line.split()
            if vg_size_GiB > disk_size_gib + 5:  # Always keep a few GiB free
                found_vg = vg_name
                break
    else:
        raise StorageError('Not enough free space in VGs!')
    hv.run(cmd(
        'lvcreate -L {0}g -n {1} {2}',
        disk_size_gib,
        vm.hostname,
        vg_name,
    ))
    return '/dev/{}/{}'.format(found_vg, vm.hostname)


def mount_temp(host, device, suffix=''):
    mount_dir = host.run(cmd('mktemp -d --suffix {0}', suffix))
    host.run(cmd('mount {0} {1}', device, mount_dir))
    return mount_dir


def umount_temp(host, device_or_path):
    host.run(cmd('umount {0}', device_or_path))


def remove_temp(host, mount_path):
    host.run(cmd('rm -rf {0}', mount_path))


def format_storage(hv, device):
    hv.run(cmd('mkfs.xfs -f {}', device))


def check_netcat(port):
    if run('pgrep -f "^/bin/nc.traditional -l -p {}"'.format(port)):
        raise StorageError(
            'Listening netcat already found on destination hypervisor.'
        )


def kill_netcat(port):
    run('pkill -f "^/bin/nc.traditional -l -p {}"'.format(port))


def netcat_to_device(host, device):
    dev_minor = host.run(cmd('stat -L -c "%T" {}', device), silent=True)
    dev_minor = int(dev_minor, 16)
    port = 7000 + dev_minor

    # Using DD lowers load on device with big enough Block Size
    host.run(
        'nohup /bin/nc.traditional -l -p {0} | dd of={1} obs=1048576 &'
        .format(port, device)
    )
    return (host.hostname, port)


def device_to_netcat(host, device, size, listener):
    # Using DD lowers load on device with big enough Block Size
    (dst_host, dst_port) = listener
    host.run(
        'dd if={0} ibs=1048576 | pv -f -s {1} '
        '| /bin/nc.traditional -q 1 {2} {3}'
        .format(device, size, dst_host, dst_port)
    )
