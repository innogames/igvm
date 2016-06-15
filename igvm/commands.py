#
# igvm - InnoGames VM Management Tool
#
# Copyright (c) 2016, InnoGames GmbH
#

"""VM resources management routines"""
import logging

from fabric.api import run, settings

from igvm.exceptions import InvalidStateError
from igvm.settings import COMMON_FABRIC_SETTINGS
from igvm.utils.storage import lvresize, get_vm_volume
from igvm.utils.units import parse_size
from igvm.vm import VM

log = logging.getLogger(__name__)


def with_fabric_settings(fn):
    """Decorator to run a function with COMMON_FABRIC_SETTINGS."""
    def decorator(*args, **kwargs):
        with settings(**COMMON_FABRIC_SETTINGS):
            return fn(*args, **kwargs)
    return decorator


def _check_defined(vm):
    if not vm.hypervisor.vm_defined(vm):
        raise InvalidStateError(
            '{} is not built yet or is not actually running on {}'
            .format(vm.hostname, vm.hypervisor.hostname)
        )


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
    _check_defined(vm)

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
    _check_defined(vm)

    current_size_gib = vm.admintool['disk_size_gib']
    if size.startswith('+'):
        new_size_gib = current_size_gib + parse_size(size[1:], 'g')
    elif not size.startswith('-'):
        new_size_gib = parse_size(size, 'g')

    if size.startswith('-') or new_size_gib < current_size_gib:
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


@with_fabric_settings
def vm_start(vm_hostname):
    vm = VM(vm_hostname)
    _check_defined(vm)

    if vm.is_running():
        log.info('{} is already running.'.format(vm.hostname))
        return
    vm.start()


@with_fabric_settings
def vm_stop(vm_hostname, force):
    vm = VM(vm_hostname)
    _check_defined(vm)

    if not vm.is_running():
        log.info('{} is already stopped.'.format(vm.hostname))
        return
    if force:
        vm.hypervisor.stop_vm_force(vm)
    else:
        vm.shutdown()
    log.info('{} stopped.'.format(vm.hostname))


@with_fabric_settings
def vm_restart(vm_hostname, force):
    vm = VM(vm_hostname)
    _check_defined(vm)

    if not vm.is_running():
        raise InvalidStateError('{} is not running'.format(vm.hostname))

    if force:
        vm.hypervisor.stop_vm_force(vm)
    else:
        vm.shutdown()

    vm.start()
    log.info('{} restarted.'.format(vm.hostname))


@with_fabric_settings
def vm_delete(vm_hostname):
    vm = VM(vm_hostname)
    _check_defined(vm)

    if vm.is_running():
        raise InvalidStateError(
            '{} is still running. Please stop it first.'.format(vm.hostname)
        )
    vm.hypervisor.undefine_vm(vm)
    vm.hypervisor.destroy_vm_storage(vm)

    vm.admintool['state'] = 'retired'
    vm.admintool.commit()
    log.info('{} destroyed and set to "retired" state.'.format(vm.hostname))
