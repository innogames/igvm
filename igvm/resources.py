#
# igvm - InnoGames VM Management Tool
#
# Copyright (c) 2016, InnoGames GmbH
#

"""VM resources management routines"""

from fabric.api import run, settings

from igvm.host import get_server
from igvm.utils.storage import lvresize, get_vm_volume


def disk_set(vm_hostname, size):
    """Change the disk size of a VM

    Currently only increasing the disk is implemented.  Size argument is
    allowed as text, but it must always be in GiBs without a decimal
    place.  The plus (+) and minus (-) prefixes are allowed to specify
    a relative difference in the size.  Of course, minus is going to
    error out.
    """

    vm = get_server(vm_hostname, 'vm')
    hypervisor = get_server(vm['xen_host'], 'hypervisor')

    if size.startswith('+'):
        new_size_gib = vm['disk_size_gib'] + parse_size(size[1:])
    elif not size.startswith('-'):
        new_size_gib = parse_size(size)

    if size.startswith('-') or new_size_gib < vm['disk_size_gib']:
        raise NotImplementedError('Cannot shrink the disk.')
    if new_size_gib == vm['disk_size_gib']:
        raise Warning('Disk size is the same.')

    with settings(host_string=hypervisor['hostname']):
        vm_volume = get_vm_volume(vm)
        lvresize(vm_volume, new_size_gib)

        # TODO This should go to utils/hypervisor.py.
        run('virsh blockresize --path {0} --size {1}GiB {2}'.format(
            vm_volume, new_size_gib, vm['hostname']
        ))

    with settings(host_string=vm['hostname']):
        # TODO This should go to utils/vm.py.
        run('xfs_growfs /')

    vm['disk_size_gib'] = new_size_gib
    vm.commit()


def parse_size(text):
    """Return the size as integer

    The GiB prefix is allowed as long as long as not ambiguous.
    """

    # First, handle the suffixes
    text = text.lower()
    if text.endswith('b'):
        text = text[:-1]
        if text.endswith('i'):
            text = text[:-1]
    if text.endswith('g'):
        text = text[:-1]
    text = text.strip()

    if not unicode(text).isnumeric():
        raise ValueError("Size has to be in GiB without decimal place.")

    return int(text)
