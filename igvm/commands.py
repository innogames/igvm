#
# igvm - InnoGames VM Management Tool
#
# Copyright (c) 2016, InnoGames GmbH
#

"""IGVM command routines"""
import logging
import sys

from fabric.api import run, settings
from fabric.colors import green, red, white, yellow
from fabric.network import disconnect_all

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
def vcpu_set(vm_hostname, count, offline=False):
    """Changes the number of CPUs in a VM."""
    vm = VM(vm_hostname)
    _check_defined(vm)

    if offline and not vm.is_running():
        log.info(
            '{} is already powered off, ignoring --offline.'
            .format(vm.hostname)
        )
        offline = False

    if count == vm.admintool['num_cpu']:
        raise Warning('CPU count is the same.')

    if offline:
        vm.shutdown()
    vm.set_num_cpu(count)
    if offline:
        vm.start()


@with_fabric_settings
def mem_set(vm_hostname, size, offline=False):
    """Changes the memory size of a VM.

    Size argument is a size unit, which defaults to MiB.
    The plus (+) and minus (-) prefixes are allowed to specify a relative
    difference in the size.  Reducing memory is only allowed while the VM is
    powered off.
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

    if offline and not vm.is_running():
        log.info(
            '{} is already powered off, ignoring --offline.'
            .format(vm.hostname)
        )
        offline = False

    if offline:
        vm.shutdown()
    vm.set_memory(new_memory)
    if offline:
        vm.start()


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
        if vm.hypervisor.admintool['hypervisor'] == 'kvm':
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
def vm_stop(vm_hostname, force=False):
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
def vm_restart(vm_hostname, force=False):
    vm = VM(vm_hostname)
    _check_defined(vm)

    if not vm.is_running():
        raise InvalidStateError('{} is not running'.format(vm.hostname))

    if force:
        vm.hypervisor.stop_vm_force(vm)
        vm.disconnect()
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


@with_fabric_settings
def vm_sync(vm_hostname):
    """Synchronize VM resource attributes to Serveradmin.

    This command collects actual resource allocation of a VM from the
    hypervisor and overwrites outdated attribute values in Serveradmin."""
    vm = VM(vm_hostname)
    _check_defined(vm)

    attributes = vm.hypervisor.vm_sync_from_hypervisor(vm)
    changed = []
    for attrib, value in attributes.iteritems():
        current = vm.admintool.get(attrib)
        if current == value:
            log.info('{}: {}'.format(attrib, current))
            continue
        log.info('{}: {} -> {}'.format(attrib, current, value))
        vm.admintool[attrib] = value
        changed.append(attrib)
    if changed:
        vm.admintool.commit()
        log.info(
            '{}: Synchronized {} attributes ({}).'
            .format(vm.hostname, len(changed), ', '.join(changed))
        )
    else:
        log.info(
            '{}: Serveradmin is already synchronized.'
            .format(vm.hostname)
        )


def _color(s, color, bold=False):
    if not sys.stdout.isatty():
        return s
    return color(s, bold=bold)


@with_fabric_settings
def host_info(vm_hostname):
    """Extracts runtime information about a VM.
    Library consumers should use VM.info() directly."""
    vm = VM(vm_hostname)

    info = vm.info()

    # Disconnect fabric now to avoid messages after the table
    disconnect_all()

    categories = (
        ('General', (
            'hypervisor',
            'status',
        )),
        ('Network', (
            'intern_ip',
            'mac_address',
        )),
        ('Resources', (
            'num_cpu',
            'max_cpus',
            'memory',
            'memory_free',
            'max_mem',
            'disk',
            'disk_size_gib',
            'disk_free_gib',
        )),
        # Anything else will appear in this section
        ('Other', None),
    )

    def _progress_bar(free_key, capacity_key, result_key, unit):
        """Helper to show nice progress bars."""
        if free_key not in info or capacity_key not in info:
            return
        free = info[free_key]
        del info[free_key]
        capacity = info[capacity_key]
        del info[capacity_key]

        simple_stats = (
            'Current: {} {unit}\n'
            'Free:    {} {unit}\n'
            'Max:     {} {unit}'
            .format(capacity - free, free, capacity, unit=unit)
        )

        if not 0 <= free <= capacity > 0:
            log.warning(
                '{} ({}) and {} ({}) have weird ratio, skipping progress '
                'calculation'
                .format(free_key, free, capacity_key, capacity)
            )
            info[result_key] = _color(simple_stats, red)
            return

        assert free >= 0 and free <= capacity
        ratio = 1 - float(free) / float(capacity)
        if ratio >= 0.9:
            color = red
        elif ratio >= 0.8:
            color = yellow
        else:
            color = green

        max_bars = 20
        num_bars = int(round(ratio * max_bars))
        info[result_key] = (
            '[{}{}] {}%\n{}'
            .format(
                _color('#' * num_bars, color), ' ' * (max_bars - num_bars),
                int(round(ratio * 100)),
                simple_stats,
            )
        )

    _progress_bar('memory_free', 'memory', 'memory', 'MiB')
    _progress_bar('disk_free_gib', 'disk_size_gib', 'disk', 'GiB')

    max_key_len = max(len(k) for k in info.keys())
    for category, keys in categories:
        # Handle 'Other' section by defaulting to all keys
        keys = keys or info.keys()

        # Any info available for the category?
        if not any(k in info for k in keys):
            continue

        print('')
        print(_color(category, white, bold=True))
        for k in keys:
            if k not in info:
                continue

            # Properly re-indent multiline values
            value = str(info.pop(k))
            value = ('\n'+' '*(max_key_len + 3)).join(value.splitlines())
            print('{} : {}'.format(k.ljust(max_key_len), value))
