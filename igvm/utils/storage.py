from __future__ import division

import logging
import math

from fabric.api import run

from igvm.exceptions import StorageError
from igvm.utils import cmd

log = logging.getLogger(__name__)

VG_NAME = 'xen-data'
RESERVED_DISK = 5.0


def get_logical_volumes(host):
    lvolumes = []
    lvs = host.run(
        'lvs --noheadings -o name,vg_name,lv_size --unit b --nosuffix'
        ' 2>/dev/null',
        silent=True
    )
    for lv_line in lvs.splitlines():
        lv_name, vg_name, lv_size = lv_line.split()
        lvolumes.append({
            'path': '/dev/{}/{}'.format(vg_name, lv_name),
            'name': lv_name,
            'vg_name': vg_name,
            'size_MiB': math.ceil(float(lv_size) / 1024 ** 2),
        })
    return lvolumes


def get_vm_volume(hypervisor, vm):
    """Return the path of the LV belonging to the given VM"""
    for lv in get_logical_volumes(hypervisor):
        if lv['name'] == vm.hostname:
            disk_size = vm.server_obj['disk_size_gib']
            if disk_size != int(math.ceil(lv['size_MiB'] / 1024)):
                raise StorageError(
                    "Server disk_size_gib {0} on Serveradmin doesn't "
                    'match the volume size {1} MiB.'
                    .format(disk_size, lv['size_MiB'])
                )
            return lv['path']
    raise StorageError('Unable to find source LV of {}'.format(vm.hostname))


def remove_logical_volume(host, lv):
    host.run(cmd('lvremove -f {0}', lv))


def lvresize(volume, size_gib):
    """Extend the volume, return the new size"""

    run('lvresize {0} -L {1}g'.format(volume, size_gib))


def lvrename(volume, newname):
    run('lvrename {0} {1}'.format(volume, newname))


def get_free_disk_size_gib(hypervisor, safe=True):
    """Return free disk space as float in GiB"""
    vgs_line = hypervisor.run(
        'vgs --noheadings -o vg_name,vg_free --unit g --nosuffix {0}'
        ' 2>/dev/null'
        .format(VG_NAME),
        silent=True,
    )
    vg_name, vg_size_gib = vgs_line.split()
    vg_size_gib = float(vg_size_gib)
    if safe is True:
        vg_size_gib -= RESERVED_DISK
    assert vg_name == VG_NAME
    return vg_size_gib


def create_storage(hypervisor, vm):
    disk_size_gib = vm.server_obj['disk_size_gib']
    hypervisor.run(cmd(
        'lvcreate -L {0}g -n {1} {2}',
        disk_size_gib,
        vm.hostname,
        VG_NAME,
    ))
    return '/dev/{}/{}'.format(VG_NAME, vm.hostname)


def mount_temp(host, device, suffix=''):
    mount_dir = host.run(cmd('mktemp -d --suffix {0}', suffix))
    host.run(cmd('mount {0} {1}', device, mount_dir))
    return mount_dir


def umount_temp(host, device_or_path):
    host.run(cmd('umount {0}', device_or_path))


def remove_temp(host, mount_path):
    host.run(cmd('rm -rf {0}', mount_path))


def format_storage(hypervisor, device):
    hypervisor.run(cmd('mkfs.xfs -f {}', device))


def _check_netcat(host, port):
    pid = host.run(
        'pgrep -f "^/bin/nc.traditional -l -p {}"'
        .format(port),
        warn_only=True,
        silent=True
    )

    if pid:
        raise StorageError(
            'Listening netcat already found on destination hypervisor.'
        )


def _kill_netcat(host, port):
    host.run('pkill -f "^/bin/nc.traditional -l -p {}"'.format(port))


def netcat_to_device(host, device, tx=None):
    dev_minor = host.run(cmd('stat -L -c "%T" {}', device), silent=True)
    dev_minor = int(dev_minor, 16)
    port = 7000 + dev_minor

    _check_netcat(host, port)

    # Using DD lowers load on device with big enough Block Size
    host.run(
        'nohup /bin/nc.traditional -l -p {0} | dd of={1} obs=1048576 &'
        .format(port, device)
    )
    if tx:
        tx.on_rollback('kill netcat', _kill_netcat, host, port)
    return (host.hostname, port)


def device_to_netcat(host, device, size, listener, tx=None):
    # Using DD lowers load on device with big enough Block Size
    host, port = listener
    host.run(
        'dd if={0} ibs=1048576 | pv -f -s {1} '
        '| /bin/nc.traditional -q 1 {2} {3}'
        .format(device, size, host, port)
    )
