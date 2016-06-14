#
# igvm - InnoGames VM Management Tool
#
# Copyright (c) 2016, InnoGames GmbH
#

"""VM resources management routines"""

from fabric.api import run, settings

from igvm.host import get_server
from igvm.settings import COMMON_FABRIC_SETTINGS
from igvm.utils.storage import lvresize, get_vm_volume
from igvm.vm import VM


def with_fabric_settings(fn):
    """Decorator to run a function with COMMON_FABRIC_SETTINGS."""
    def decorator(*args, **kwargs):
        with settings(**COMMON_FABRIC_SETTINGS):
            return fn(*args, **kwargs)
    return decorator


@with_fabric_settings
def mem_set(vm_hostname, size):
    """Changes the memory size of a VM.

    Currently only increasing the disk is implemented.  Size argument is
    allowed as text, but it must always be in MiBs without a decimal
    place.  The plus (+) and minus (-) prefixes are allowed to specify
    a relative difference in the size.  Of course, minus is going to
    error out.
    """
    vm = VM(vm_hostname)

    if size.startswith('+'):
        new_memory = vm.admintool['memory'] + parse_size(size[1:], 'm')
    elif size.startswith('-'):
        new_memory = vm.admintool['memory'] - parse_size(size[1:], 'm')
    else:
        new_memory = parse_size(size, 'm')

    if new_memory == vm.admintool['memory']:
        raise Warning('Memory size is the same.')

    vm.set_memory(new_memory)


@with_fabric_settings
def disk_set(vm_hostname, size):
    """Change the disk size of a VM

    Currently only increasing the disk is implemented.  Size argument is
    allowed as text, but it must always be in GiBs without a decimal
    place.  The plus (+) and minus (-) prefixes are allowed to specify
    a relative difference in the size.  Of course, minus is going to
    error out.
    """
    vm = VM(vm_hostname)

    if size.startswith('+'):
        new_size_gib = vm.admintool['disk_size_gib'] + parse_size(size[1:], 'g')
    elif not size.startswith('-'):
        new_size_gib = parse_size(size, 'g')

    if size.startswith('-') or new_size_gib < vm.admintool['disk_size_gib']:
        raise NotImplementedError('Cannot shrink the disk.')
    if new_size_gib == vm.admintool['disk_size_gib']:
        raise Warning('Disk size is the same.')

    with vm.hypervisor.fabric_settings():
        vm_volume = get_vm_volume(vm.hypervisor, vm)
        lvresize(vm_volume, new_size_gib)

        # TODO This should go to utils/hypervisor.py.
        run('virsh blockresize --path {0} --size {1}GiB {2}'.format(
            vm_volume, new_size_gib, vm.hostname
        ))

    # TODO This should go to utils/vm.py.
    vm.run('xfs_growfs /')

    vm.admintool['disk_size_gib'] = new_size_gib
    vm.admintool.commit()


def parse_size(text, unit):
    """Return the size as integer in the desired unit.

    The TiB/GiB/MiB/KiB prefix is allowed as long as long as not ambiguous.
    We are dealing with the units case in-sensitively.
    """

    # First, handle the suffixes
    text = text.lower()

    if text.endswith('b'):
        text = text[:-1]
        if text.endswith('i'):
            text = text[:-1]

    if not text:
        return ValueError('Empty size')

    FACTORS = {
        't': 1024**4,
        'g': 1024**3,
        'm': 1024**2,
        'k': 1024,
    }

    if text[-1] in FACTORS:
        factor = FACTORS[text[-1]]
        text = text[:-1]
    else:
        factor = FACTORS[unit]

    text = text.strip()

    if not unicode(text).isnumeric():
        raise ValueError(
            'Size has to be in {}iB without decimal place.'
            .format(unit.upper())
        )

    value = int(text) * factor
    if value % FACTORS[unit]:
        raise ValueError('Value must be multiple of 1 {}iB'.format(unit.upper()))
    return int(value / FACTORS[unit])
