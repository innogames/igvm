from __future__ import division

import math

from fabric.api import run, settings

from igvm.utils import cmd, ManageVMError


class StorageError(ManageVMError):
    pass


def get_logical_volumes():
    lvolumes = []
    for lv_line in run('lvs --noheadings -o name,vg_name,lv_size --unit m --nosuffix').splitlines():
        lv_name, vg_name, lv_size = lv_line.split()
        lvolumes.append({
            'path': '/dev/{}/{}'.format(vg_name, lv_name),
            'name': lv_name,
            'vg_name': vg_name,
            'size_MiB': math.ceil(float(lv_size)),
        })
    return lvolumes


def remove_logical_volume(lv):
    run('lvremove -f {0}'. format(lv))


def lvresize(volume, size_gib):
    """Extend the volume, return the new size"""

    run('lvresize {0} -L {1}g'.format(volume, size_gib))


def create_storage(vm_name, size_gib):
    # Do not search only for the given LV.
    # `lvs` must generally not fail and give a list of LVs.
    for lv_line in run('lvs --noheading -o vg_name,name').splitlines():
        vg_name, lv_name = lv_line.split()
        if lv_name == vm_name:
            raise StorageError('Logical Volume {}/{} already exists!'.format(vg_name, lv_name))
    # Find VG with enough free space
    for vg_line in run('vgs --noheadings -o vg_name,vg_free --unit g --nosuffix').splitlines():
            vg_name, vg_size_gib = vg_line.split()
            if vg_size_gib > size_gib + 5: # Always keep a few GiB free
                found_vg = vg_name
                break
    else:
        raise StorageError('Not enough free space in VGs!')
    with settings(warn_only=True):
        out = run(cmd('lvcreate -L {0}g -n {1} {2}', size_gib, vm_name, vg_name))
        if out.failed:
            raise StorageError('Unable to create Logical Volume {}/{}!'.format(volume_group, name))
    return '/dev/{}/{}'.format(found_vg, vm_name)


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
        raise StorageError((
            'VM block device name unknown for hypervisor {0}'
        ).format(hypervisor))


def mount_storage(device, hostname):
    # First, make the file system
    run(cmd('mkfs.xfs -f {0}', device))
    mount_path = mount_temp(device, suffix='-' + hostname)
    return mount_path


def check_netcat(port):
    if run('pgrep -f "^/bin/nc.traditional -l -p {}"'.format(port)):
        raise StorageError('Listening netcat already found on destination hypervisor.')


def kill_netcat(port):
    run('pkill -f "^/bin/nc.traditional -l -p {}"'.format(port))


def netcat_to_device(device):
    dev_minor = run('stat -L -c "%T" {}'.format(device))
    dev_minor = int(dev_minor, 16)
    port = 7000 + dev_minor
    # Using DD lowers load on device with big enough Block Size
    run('nohup /bin/nc.traditional -l -p {0} | dd of={1} obs=1048576 &'.format(port, device))
    return port


def device_to_netcat(device, size, host, port):
    # Using DD lowers load on device with big enough Block Size
    with settings(warn_only=True):
        out = run('dd if={0} ibs=1048576 | pv -f -s {1} | /bin/nc.traditional -q 1 {2} {3}'.format(device, size, host, port))
        if out.failed:
            raise StorageError('Copying data over NetCat has failed!')
