"""igvm - Command Routines

Copyright (c) 2018 InnoGames GmbH
"""

import logging
from os import environ
from contextlib import contextmanager, ExitStack

from adminapi.dataset import Query
from adminapi.filters import Any, StartsWith
from fabric.colors import green, red, white, yellow
from fabric.network import disconnect_all
from libvirt import libvirtError

from igvm.exceptions import (
    HypervisorError,
    IGVMError,
    InconsistentAttributeError,
    InvalidStateError,
)
from igvm.host import with_fabric_settings
from igvm.hypervisor import Hypervisor
from igvm.hypervisor_preferences import sorted_hypervisors
from igvm.settings import (
    HYPERVISOR_ATTRIBUTES,
    HYPERVISOR_PREFERENCES,
    VM_ATTRIBUTES,
)
from igvm.transaction import Transaction
from igvm.utils import parse_size
from igvm.vm import VM

log = logging.getLogger(__name__)


def _check_defined(vm, fail_hard=True):
    error = None

    if not vm.hypervisor:
        error = ('"{}" has no hypervisor defined. Use --force to ignore this'
                 .format(vm.fqdn))
    elif not vm.hypervisor.vm_defined(vm):
        error = ('"{}" is not built yet or is not running on "{}"'
                 .format(vm.fqdn, vm.hypervisor.fqdn))

    if error:
        if fail_hard:
            raise InvalidStateError(error)
        else:
            log.info(error)


@with_fabric_settings
def vcpu_set(vm_hostname, count, offline=False, ignore_reserved=False):
    """Change the number of CPUs in a VM"""
    with ExitStack() as es:
        vm = es.enter_context(
            _get_vm(vm_hostname, ignore_reserved=ignore_reserved)
        )

        _check_defined(vm)

        if offline and not vm.is_running():
            log.info(
                '"{}" is already powered off, ignoring --offline.'
                .format(vm.fqdn)
            )
            offline = False

        if count == vm.dataset_obj['num_cpu']:
            raise Warning('CPU count is the same.')

        if offline:
            vm.shutdown()
        vm.set_num_cpu(count)
        if offline:
            vm.start()


@with_fabric_settings
def mem_set(vm_hostname, size, offline=False, ignore_reserved=False):
    """Change the memory size of a VM

    Size argument is a size unit, which defaults to MiB.
    The plus (+) and minus (-) prefixes are allowed to specify a relative
    difference in the size.  Reducing memory is only allowed while the VM is
    powered off.
    """
    with ExitStack() as es:
        vm = es.enter_context(
            _get_vm(vm_hostname, ignore_reserved=ignore_reserved)
        )

        _check_defined(vm)

        if size.startswith('+'):
            new_memory = vm.dataset_obj['memory'] + parse_size(size[1:], 'm')
        elif size.startswith('-'):
            new_memory = vm.dataset_obj['memory'] - parse_size(size[1:], 'm')
        else:
            new_memory = parse_size(size, 'm')

        if new_memory == vm.dataset_obj['memory']:
            raise Warning('Memory size is the same.')

        if offline and not vm.is_running():
            log.info(
                '"{}" is already powered off, ignoring --offline.'
                .format(vm.fqdn)
            )
            offline = False

        if offline:
            vm.shutdown()
        vm.set_memory(new_memory)
        if offline:
            vm.start()


@with_fabric_settings
def disk_set(vm_hostname, size, ignore_reserved=False):
    """Change the disk size of a VM

    Currently only increasing the disk is implemented.  Size argument is
    allowed as text, but it must always be in GiBs without a decimal
    place.  The plus (+) and minus (-) prefixes are allowed to specify
    a relative difference in the size.  Of course, minus is going to
    error out.
    """
    with ExitStack() as es:
        vm = es.enter_context(
            _get_vm(vm_hostname, ignore_reserved=ignore_reserved)
        )

        _check_defined(vm)

        current_size_gib = vm.dataset_obj['disk_size_gib']
        if size.startswith('+'):
            new_size_gib = current_size_gib + parse_size(size[1:], 'g')
        elif size.startswith('-'):
            new_size_gib = current_size_gib - parse_size(size[1:], 'g')
        else:
            new_size_gib = parse_size(size, 'g')

        if new_size_gib == vm.dataset_obj['disk_size_gib']:
            raise Warning('Disk size is the same.')

        vm.hypervisor.vm_set_disk_size_gib(vm, new_size_gib)

        vm.dataset_obj['disk_size_gib'] = new_size_gib
        vm.dataset_obj.commit()


@with_fabric_settings
def vm_build(vm_hostname, run_puppet=True, debug_puppet=False, postboot=None,
             ignore_reserved=False, rebuild=False):
    """Create a VM and start it

    Puppet in run once to configure baseline networking.
    """

    with ExitStack() as es:
        vm = es.enter_context(_get_vm(vm_hostname))

        if not vm.hypervisor:
            vm.hypervisor = es.enter_context(_get_best_hypervisor(
                vm,
                ['online', 'online_reserved'] if ignore_reserved
                else ['online'],
                True,
            ))
            vm.dataset_obj['hypervisor'] = \
                vm.hypervisor.dataset_obj['hostname']

        if vm.hypervisor.vm_defined(vm) and vm.is_running():
            raise InvalidStateError(
                '"{}" is still running.'.format(vm.fqdn)
            )

        es.enter_context(_lock_hv(vm.hypervisor))

        if rebuild and vm.hypervisor.vm_defined(vm):
            vm.hypervisor.undefine_vm(vm)

        vm.build(
            run_puppet=run_puppet,
            debug_puppet=debug_puppet,
            postboot=postboot,
        )

        vm.dataset_obj.commit()


@with_fabric_settings   # NOQA: C901
def vm_migrate(vm_hostname, hypervisor_hostname=None, newip=None,
               run_puppet=False, debug_puppet=False,
               offline=False, offline_transport='drbd', ignore_reserved=False):
    """Migrate a VM to a new hypervisor."""
    with ExitStack() as es:
        vm = es.enter_context(
            _get_vm(vm_hostname, ignore_reserved=ignore_reserved)
        )
        if hypervisor_hostname:
            hypervisor = es.enter_context(_get_hypervisor(
                hypervisor_hostname, ignore_reserved=ignore_reserved
            ))
            if vm.hypervisor.fqdn == hypervisor.fqdn:
                raise IGVMError(
                    'Source and destination Hypervisor is the same!'
                )
        else:
            hypervisor = es.enter_context(_get_best_hypervisor(
                vm,
                ['online', 'online_reserved'] if ignore_reserved
                else ['online'],
                offline,
            ))

        was_running = vm.is_running()

        # There is no point of online migration, if the VM is already shutdown.
        if not was_running:
            offline = True

        if not offline and newip:
            raise IGVMError('Online migration cannot change IP address.')

        if not offline and run_puppet:
            raise IGVMError('Online migration cannot run Puppet.')

        if not run_puppet and newip:
            raise IGVMError(
                'Changing IP requires a Puppet run, pass --run-puppet.'
            )

        # Validate destination hypervisor can run the VM (needs to happen after
        # setting new IP!)
        hypervisor.check_vm(vm, offline)

        # Require VM to be in sync with serveradmin
        _check_attributes(vm)

        vm.check_serveradmin_config()

        if newip:
            vm._set_ip(newip)

        with Transaction() as transaction:
            # Commit previously changed IP address.
            if newip:
                # TODO: This commit is not rolled back.
                vm.dataset_obj.commit()
                transaction.on_rollback(
                    'newip warning', log.info, '--newip is not rolled back'
                )

            vm.hypervisor.migrate_vm(
                vm, hypervisor, offline, offline_transport, transaction
            )

            previous_hypervisor = vm.hypervisor
            vm.hypervisor = hypervisor

            def _reset_hypervisor():
                vm.hypervisor = previous_hypervisor
            transaction.on_rollback('reset hypervisor', _reset_hypervisor)

            if run_puppet:
                hypervisor.mount_vm_storage(vm, transaction)
                vm.run_puppet(debug=debug_puppet)
                hypervisor.umount_vm_storage(vm)

            if offline and was_running:
                vm.start(transaction=transaction)
            vm.reset_state()

            # Update Serveradmin
            vm.dataset_obj['hypervisor'] = hypervisor.dataset_obj['hostname']
            vm.dataset_obj.commit()

        # If removing the existing VM fails we shouldn't risk undoing the newly
        # migrated one.
        previous_hypervisor.undefine_vm(vm)


@with_fabric_settings
def vm_start(vm_hostname):
    """Start a VM"""
    with _get_vm(vm_hostname) as vm:
        _check_defined(vm)

        if vm.is_running():
            log.info('"{}" is already running.'.format(vm.fqdn))
            return
        vm.start()


@with_fabric_settings
def vm_stop(vm_hostname, force=False):
    """Gracefully stop a VM"""
    with _get_vm(vm_hostname) as vm:
        _check_defined(vm)

        if not vm.is_running():
            log.info('"{}" is already stopped.'.format(vm.fqdn))
            return
        if force:
            vm.hypervisor.stop_vm_force(vm)
        else:
            vm.shutdown()
        log.info('"{}" is stopped.'.format(vm.fqdn))


@with_fabric_settings
def vm_restart(vm_hostname, force=False, no_redefine=False):
    """Restart a VM

    The VM is shut down and recreated, using the existing disk. This can be
    useful to discard temporary changes or adapt new hypervisor optimizations.
    No data will be lost.
    """
    with ExitStack() as es:
        vm = es.enter_context(_get_vm(vm_hostname, ignore_reserved=True))

        _check_defined(vm)

        if not vm.is_running():
            raise InvalidStateError('"{}" is not running'.format(vm.fqdn))

        if force:
            vm.hypervisor.stop_vm_force(vm)
        else:
            vm.shutdown()

        if not no_redefine:
            vm.hypervisor.redefine_vm(vm)

        vm.start()
        log.info('"{}" is restarted.'.format(vm.fqdn))


@with_fabric_settings
def vm_delete(vm_hostname, retire=False):
    """Delete the VM from the hypervisor and from serveradmin

    If retire is True the VM will not be deleted from serveradmin but it's
    state will be updated to 'retired'.
    """

    with _get_vm(vm_hostname, ignore_reserved=True, unlock=retire) as vm:
        # Make sure the VM has a hypervisor and that it is defined on it.
        # Abort if the VM has not been defined.
        _check_defined(vm)

        # Make sure the VM is shut down, abort if it is not.
        if vm.hypervisor and vm.hypervisor.vm_defined(vm) and vm.is_running():
            raise InvalidStateError('"{}" is still running.'.format(vm.fqdn))

        # Delete the VM from its hypervisor if required.
        if vm.hypervisor and vm.hypervisor.vm_defined(vm):
            vm.hypervisor.undefine_vm(vm)

        # Delete the serveradmin object of this VM
        # or update its state to 'retired' if retire is True.
        if retire:
            vm.dataset_obj['state'] = 'retired'
            vm.dataset_obj.commit()
            log.info(
                '"{}" is destroyed and set to "retired" state.'
                .format(vm.fqdn)
            )
        else:
            vm.dataset_obj.delete()
            vm.dataset_obj.commit()
            log.info(
                '"{}" is destroyed and deleted from Serveradmin'
                .format(vm.fqdn)
            )


@with_fabric_settings
def vm_sync(vm_hostname):
    """Synchronize VM resource attributes to Serveradmin

    This command collects actual resource allocation of a VM from the
    hypervisor and overwrites outdated attribute values in Serveradmin."""
    with _get_vm(vm_hostname, ignore_reserved=True) as vm:
        _check_defined(vm)

        attributes = vm.hypervisor.vm_sync_from_hypervisor(vm)
        changed = []
        for attrib, value in attributes.items():
            current = vm.dataset_obj[attrib]
            if current == value:
                log.info('{}: {}'.format(attrib, current))
                continue
            log.info('{}: {} -> {}'.format(attrib, current, value))
            vm.dataset_obj[attrib] = value
            changed.append(attrib)
        if changed:
            vm.dataset_obj.commit()
            log.info(
                '"{}" is synchronized {} attributes ({}).'
                .format(vm.fqdn, len(changed), ', '.join(changed))
            )
        else:
            log.info(
                '"{}" is already synchronized on Serveradmin.'.format(vm.fqdn)
            )


@with_fabric_settings   # NOQA: C901
def host_info(vm_hostname):
    """Extract runtime information about a VM

    Library consumers should use VM.info() directly.
    """
    with _get_vm(vm_hostname, ignore_reserved=True) as vm:

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
                info[result_key] = red(simple_stats)
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
                    color('#' * num_bars), ' ' * (max_bars - num_bars),
                    int(round(ratio * 100)),
                    simple_stats,
                )
            )

        _progress_bar('memory_free', 'memory', 'memory', 'MiB')
        _progress_bar('disk_free_gib', 'disk_size_gib', 'disk', 'GiB')

        max_key_len = max(len(k) for k in info.keys())
        for category, keys in categories:
            # Handle 'Other' section by defaulting to all keys
            keys = list(keys or info.keys())

            # Any info available for the category?
            if not any(k in info for k in keys):
                continue

            print('')
            print(white(category, bold=True))
            for k in keys:
                if k not in info:
                    continue

                # Properly re-indent multiline values
                value = str(info.pop(k))
                value = ('\n' + ' ' * (max_key_len + 3)).join(
                    value.splitlines()
                )
                print('{} : {}'.format(k.ljust(max_key_len), value))


@with_fabric_settings
def vm_rename(vm_hostname, new_hostname, offline=False):
    """Redefine the VM on the same hypervisor with a different name

    We can only do this operation offline.  If the VM is online, it needs
    to be shut down.  No data will be lost.
    """

    with _get_vm(vm_hostname, ignore_reserved=True) as vm:
        _check_defined(vm)

        if not offline:
            raise NotImplementedError(
                'Rename command only works with --offline at the moment.'
            )
        if not vm.is_running():
            raise NotImplementedError(
                'Rename command only works online at the moment.'
            )

        vm.rename(new_hostname)


@contextmanager
def _get_vm(hostname, ignore_reserved=False, unlock=True):
    """Get a server from Serveradmin by hostname to return VM object

    The function is accepting hostnames in any length as long as it resolves
    to a single server on Serveradmin.
    """
    def vm_query():
        return Query({
            'hostname': Any(hostname, StartsWith(hostname + '.')),
            'servertype': 'vm',
        }, VM_ATTRIBUTES).get()

    dataset_obj = vm_query()

    hypervisor = None
    if dataset_obj['hypervisor']:
        hypervisor = Hypervisor(dataset_obj['hypervisor'])

        # XXX: Ugly hack until adminapi supports modifying joined objects
        dict.__setitem__(
            dataset_obj, 'hypervisor', dataset_obj['hypervisor']['hostname']
        )

    vm = VM(dataset_obj, hypervisor)
    vm.acquire_lock()

    try:
        yield vm
    except (Exception, KeyboardInterrupt):
        VM(vm_query(), hypervisor).release_lock()
        raise
    else:
        # We re-fetch the VM because we can't risk commiting any other changes
        # to the VM than unlocking. There can be changes from failed things,
        # like setting memory.
        # Most operations require unlocking, the only exception is deleting of
        # a VM. After object is deleted, it can't be unlocked.
        if unlock:
            VM(vm_query(), hypervisor).release_lock()


@contextmanager
def _get_hypervisor(hostname, ignore_reserved=False):
    """Get a server from Serveradmin by hostname to return Hypervisor object"""
    dataset_obj = Query({
        'hostname': hostname,
        'servertype': 'hypervisor',
    }, HYPERVISOR_ATTRIBUTES).get()

    if not ignore_reserved and dataset_obj['state'] == 'online_reserved':
        raise InvalidStateError(
            'Server "{0}" is online_reserved.'.format(dataset_obj['hostname'])
        )

    hypervisor = Hypervisor(dataset_obj)
    # hypervisor.acquire_lock()

    try:
        yield hypervisor
    finally:
        pass
        # hypervisor.release_lock()


@contextmanager
def _get_best_hypervisor(vm, hypervisor_states, offline=False):
    hypervisors = (Hypervisor(o) for o in Query({
        'servertype': 'hypervisor',
        'environment': environ.get('IGVM_MODE', 'production'),
        'vlan_networks': vm.dataset_obj['route_network'],
        'state': Any(*hypervisor_states),
    }, HYPERVISOR_ATTRIBUTES))

    for hypervisor in sorted_hypervisors(
        HYPERVISOR_PREFERENCES, vm, hypervisors
    ):
        # The actual resources are not checked during sorting for performance.
        # We need to validate the hypervisor using the actual values before
        # the final decision.
        try:
            # hypervisor.acquire_lock()
            pass
        except InvalidStateError as error:
            log.warning(error)
            continue

        try:
            hypervisor.check_vm(vm, offline)
        except libvirtError as error:
            # hypervisor.release_lock()
            log.warning(
                'Preferred hypervisor "{}" is skipped: {}'
                .format(hypervisor, error)
            )
            continue
        except HypervisorError as error:
            # hypervisor.release_lock()
            log.warning(
                'Preferred hypervisor "{}" is skipped: {}'
                .format(hypervisor, error)
            )
            continue

        try:
            yield hypervisor
        finally:
            pass
            # hypervisor.release_lock()
        break
    else:
        raise IGVMError('Cannot find a hypervisor')


@contextmanager
def _lock_hv(hv):
    hv.acquire_lock()
    try:
        yield hv
    finally:
        hv.release_lock()


def _check_attributes(vm):
    synced_attributes = vm.hypervisor.vm_sync_from_hypervisor(vm)
    for attr, value in synced_attributes.items():
        if vm.dataset_obj[attr] != value:
            raise InconsistentAttributeError(vm, attr, value)
