"""igvm - Command Routines

Copyright (c) 2021 InnoGames GmbH
"""

import logging
import math
from collections import OrderedDict
from contextlib import contextmanager, ExitStack
from ipaddress import ip_address
from os import environ
from time import sleep
from typing import List, Optional

from adminapi import parse
from adminapi.dataset import Query
from adminapi.filters import Any, BaseFilter, StartsWith, Contains
from fabric.colors import green, red, white, yellow
from fabric.network import disconnect_all
from jinja2 import Environment, PackageLoader
from libvirt import libvirtError

from igvm import puppet
from igvm.exceptions import (
    ConfigError,
    HypervisorError,
    IGVMError,
    InconsistentAttributeError,
    InvalidStateError,
)
from igvm.host import with_fabric_settings
from igvm.hypervisor import Hypervisor
from igvm.hypervisor_preferences import sort_by_preference
from igvm.settings import (
    AWS_CONFIG,
    AWS_RETURN_CODES,
    HYPERVISOR_ATTRIBUTES,
    HYPERVISOR_PREFERENCES,
    VM_ATTRIBUTES,
)
from igvm.transaction import Transaction
from igvm.utils import parse_size, parallel
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
def evacuate(
    hv_hostname: str,
    target_hv_query: Optional[str] = None,
    offline: Optional[List[str]] = None,
    allow_reserved_hv: bool = False,
    dry_run: bool = False,
    soft_preferences: bool = False,
):
    """Move all VMs out of a hypervisor

    Move all VMs out of a hypervisor and put it to state online reserved.

    Offline can be passed without arguments or with a list strings matching
    function attributes. If set to true all VMs will be migrated offline. If
    a list of strings is passed only those matching will be migrated offline.

    It is also possible to specify a destination hypervisor and migrating to
    online reserved hypervisors can also be allowed.
    """
    with _get_hypervisor(hv_hostname, allow_reserved=True) as hv:
        if dry_run:
            log.info('I would set {} to state online_reserved'.format(
                hv_hostname)
            )
        else:
            hv.dataset_obj['state'] = 'online_reserved'
            hv.dataset_obj.commit()

        for vm in hv.dataset_obj['vms']:
            vm_function = vm['function']
            is_offline_migration = (
                offline is not None
                and (offline == [] or vm_function in offline)
            )

            state_str = 'offline' if is_offline_migration else 'online'
            if dry_run:
                log.info('Would migrate {} {}'.format(
                    vm['hostname'],
                    state_str,
                ))

                continue

            log.info('Migrating {} {}...'.format(
                vm['hostname'],
                state_str,
            ))
            vm_migrate(
                vm['hostname'],
                target_hv_query=target_hv_query,
                offline=is_offline_migration,
                allow_reserved_hv=allow_reserved_hv,
                soft_preferences=soft_preferences,
            )


@with_fabric_settings
def vcpu_set(vm_hostname, count, offline=False):
    """Change the number of CPUs in a VM"""
    with ExitStack() as es:
        vm = es.enter_context(_get_vm(vm_hostname))

        if vm.dataset_obj['datacenter_type'] != 'kvm.dct':
            raise NotImplementedError(
                'This operation is not yet supported for {}'.format(
                    vm.dataset_obj['datacenter_type'])
            )

        _check_defined(vm)

        if offline and not vm.is_running():
            log.info(
                '"{}" is already powered off, ignoring --offline.'.format(
                    vm.fqdn)
            )
            offline = False

        if str(count).startswith('+'):
            count = vm.dataset_obj['num_cpu'] + int(str(count)[1:])
        elif str(count).startswith('-'):
            if not offline:
                raise IGVMError(
                    'Decreasing CPU count is only allowed offline.'
                )
            count = vm.dataset_obj['num_cpu'] - int(str(count)[1:])
        elif int(count) == vm.dataset_obj['num_cpu']:
            raise Warning('CPU count is the same.')

        if offline:
            vm.shutdown()
        vm.set_num_cpu(int(count))
        if offline:
            vm.start()


@with_fabric_settings
def mem_set(vm_hostname, size, offline=False):
    """Change the memory size of a VM

    Size argument is a size unit, which defaults to MiB.
    The plus (+) and minus (-) prefixes are allowed to specify a relative
    difference in the size.  Reducing memory is only allowed while the VM is
    powered off.
    """
    with ExitStack() as es:
        vm = es.enter_context(_get_vm(vm_hostname))

        if vm.dataset_obj['datacenter_type'] != 'kvm.dct':
            raise NotImplementedError(
                'This operation is not yet supported for {}'.format(
                    vm.dataset_obj['datacenter_type'])
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
                '"{}" is already powered off, ignoring --offline.'.format(
                    vm.fqdn)
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
    with ExitStack() as es:
        vm = es.enter_context(_get_vm(vm_hostname))

        current_size_gib = vm.dataset_obj['disk_size_gib']
        if size.startswith('+'):
            new_size_gib = current_size_gib + parse_size(size[1:], 'g')
        elif size.startswith('-'):
            new_size_gib = current_size_gib - parse_size(size[1:], 'g')
        else:
            new_size_gib = parse_size(size, 'g')

        if new_size_gib == vm.dataset_obj['disk_size_gib']:
            raise Warning('Disk size is the same.')

        if vm.dataset_obj['datacenter_type'] == 'aws.dct':
            vm.aws_disk_set(new_size_gib)
        elif vm.dataset_obj['datacenter_type'] == 'kvm.dct':
            _check_defined(vm)

            vm.hypervisor.vm_set_disk_size_gib(vm, new_size_gib)

        else:
            raise NotImplementedError(
                'This operation is not yet supported for {}'.format(
                    vm.dataset_obj['datacenter_type'])
            )

        vm.dataset_obj['disk_size_gib'] = new_size_gib
        vm.dataset_obj.commit()


@with_fabric_settings
def change_address(
    vm_hostname, new_address,
    offline=False, migrate=False, allow_reserved_hv=False,
    offline_transport='drbd',
):
    """Change VMs IP address

    This is done by changing data in Serveradmin, running Puppet in VM and
    rebooting it.
    """

    if not offline:
        raise IGVMError('IP address change can be only performed offline')

    with _get_vm(vm_hostname) as vm:
        if vm.dataset_obj['datacenter_type'] != 'kvm.dct':
            raise NotImplementedError(
                'This operation is not yet supported for {}'.format(
                    vm.dataset_obj['datacenter_type'])
            )

        new_address = ip_address(new_address)

        if vm.dataset_obj['intern_ip'] == new_address:
            raise ConfigError('New IP address is the same as the old one!')

        if not vm.hypervisor.get_vlan_network(new_address) and not migrate:
            err = 'Current hypervisor does not support new subnet!'
            raise ConfigError(err)

        new_network = Query(
            {
                'servertype': 'route_network',
                'state': 'online',
                'network_type': 'internal',
                'intern_ip': Contains(new_address),
            }
        ).get()['hostname']

        vm_was_running = vm.is_running()

        with Transaction() as transaction:
            if vm_was_running:
                vm.shutdown(
                    transaction=transaction,
                    check_vm_up_on_transaction=False,
                )
            vm.change_address(
                new_address, new_network, transaction=transaction,
            )

            if migrate:
                vm_migrate(
                    vm_object=vm,
                    run_puppet=True,
                    offline=True,
                    no_shutdown=True,
                    allow_reserved_hv=allow_reserved_hv,
                    offline_transport=offline_transport,
                )
            else:
                vm.hypervisor.mount_vm_storage(vm, transaction=transaction)
                vm.run_puppet()
                vm.hypervisor.redefine_vm(vm)
                vm.hypervisor.umount_vm_storage(vm)

            if vm_was_running:
                vm.start()


@with_fabric_settings
def vm_build(
    vm_hostname: str,
    run_puppet: bool = True,
    debug_puppet: bool = False,
    postboot: Optional[str] = None,
    allow_reserved_hv: bool = False,
    rebuild: bool = False,
    enforce_vm_env: bool = False,
    soft_preferences: bool = False,
    barebones: bool = False,
    target_hv_query: Optional[str] = None,
):
    """Create a VM and start it

    Puppet in run once to configure baseline networking.
    """

    with ExitStack() as es:
        vm = es.enter_context(_get_vm(vm_hostname))

        if vm.dataset_obj['datacenter_type'] == 'aws.dct':
            # check if aws_image_id is our own, if yes, skip puppet_run in
            # cloud_init because the image already includes basic configs
            # like ssh keys. This is only needed for the disaster recovery
            # of the games since we want to spawn instances as fast as possible
            # in AWS in that case. Our failover scripts take care in the
            # downstream steps that the packages and configs are up to date
            is_golden = vm.is_aws_image_golden()
            jenv = Environment(loader=PackageLoader('igvm', 'templates'))
            template = jenv.get_template('aws_user_data.cfg')
            user_data = template.render(
                hostname=vm.dataset_obj['hostname'],
                fqdn=vm.dataset_obj['hostname'],
                vm_os=vm.dataset_obj['os'],
                apt_repos=AWS_CONFIG[0]['apt'],
                puppet_master=vm.dataset_obj['puppet_master'],
                puppet_ca=vm.dataset_obj['puppet_ca'],
                is_golden=is_golden,
            )

            if rebuild:
                vm.aws_delete()
                timeout_terminate = 60
                instance_status = vm.aws_describe_instance_status(
                    vm.dataset_obj['aws_instance_id'])
                while (
                    timeout_terminate and
                    AWS_RETURN_CODES['terminated'] != instance_status
                ):
                    timeout_terminate -= 1
                    sleep(1)

            vm.aws_build(
                run_puppet=run_puppet,
                debug_puppet=debug_puppet,
                postboot=user_data
            )
            attributes = vm.aws_sync()
            for attr, val in attributes.items():
                vm.dataset_obj[attr] = val
        elif vm.dataset_obj['datacenter_type'] == 'kvm.dct':
            if vm.hypervisor:
                es.enter_context(_lock_hv(vm.hypervisor))
            else:
                hv_filter = parse.parse_query(target_hv_query or '')
                vm.hypervisor = es.enter_context(_get_best_hypervisor(
                    vm,
                    ['online', 'online_reserved'] if allow_reserved_hv
                    else ['online'],
                    True,
                    enforce_vm_env,
                    soft_preferences,
                    hv_filter,
                ))
                vm.dataset_obj['hypervisor'] = \
                    vm.hypervisor.dataset_obj['hostname']

            if vm.hypervisor.vm_defined(vm) and vm.is_running():
                raise InvalidStateError(
                    '"{}" is still running.'.format(vm.fqdn)
                )

            if rebuild and vm.hypervisor.vm_defined(vm):
                vm.hypervisor.undefine_vm(vm)

            vm.build(
                run_puppet=run_puppet,
                debug_puppet=debug_puppet,
                postboot=postboot,
                cleanup_cert=rebuild,
                barebones=barebones,
            )
        else:
            raise NotImplementedError(
                'This operation is not yet supported for {}'.format(
                    vm.dataset_obj['datacenter_type'])
            )

        vm.dataset_obj.commit()


@with_fabric_settings  # NOQA: C901
def vm_migrate(
    vm_hostname: str = None,
    vm_object=None,
    target_hv_query: Optional[str] = None,
    run_puppet: bool = False,
    debug_puppet: bool = False,
    offline: bool = False,
    offline_transport: str = 'drbd',
    allow_reserved_hv: bool = False,
    no_shutdown: bool = False,
    enforce_vm_env: bool = False,
    disk_size: Optional[int] = None,
    soft_preferences: bool = False,
):
    """Migrate a VM to a new hypervisor."""

    if not (bool(vm_hostname) ^ bool(vm_object)):
        raise IGVMError(
            'Only one of vm_hostname or vm_object can be given!'
        )

    with ExitStack() as es:
        if vm_object:
            # VM given as object and hopefully already locked
            _vm = vm_object
        else:
            _vm = es.enter_context(
                _get_vm(vm_hostname, allow_retired=True)
            )

        if _vm.dataset_obj['datacenter_type'] != 'kvm.dct':
            raise NotImplementedError(
                'This operation is not yet supported for {}'.format(
                    _vm.dataset_obj['datacenter_type'])
            )

        # We have to check migration settings before searching for a HV,
        # because the new disk size must be checked and set
        original_size_gib = _vm.dataset_obj['disk_size_gib']
        _vm.dataset_obj['disk_size_gib'] = _vm.hypervisor.vm_new_disk_size(
            _vm, offline, offline_transport, disk_size
        )

        hv_filter = parse.parse_query(target_hv_query or '')
        if (
            len(hv_filter.keys()) == 1
            and 'hostname' in hv_filter
            # BaseFilter is used for scalar types like string, so it is most
            # likely that a specific hypervisor was requested. Any other filter
            # could resolve to multiple HVs.
            and (not isinstance(hv_filter['hostname'], BaseFilter)
                 or type(hv_filter['hostname']) == BaseFilter)
        ):
            hypervisor = es.enter_context(_get_hypervisor(
                hv_filter['hostname'],
                allow_reserved=allow_reserved_hv,
            ))
        else:
            hypervisor = es.enter_context(_get_best_hypervisor(
                _vm,
                ['online', 'online_reserved'] if allow_reserved_hv
                else ['online'],
                offline,
                enforce_vm_env,
                soft_preferences,
                hv_filter,
            ))

        if _vm.hypervisor.fqdn == hypervisor.fqdn:
            raise IGVMError(
                'Source and destination Hypervisor is the same!'
            )

        was_running = _vm.is_running()

        # There is no point of online migration, if the VM is already shutdown.
        if not was_running:
            log.warning(
                f'{_vm.fqdn} is already shutdown. Forcing offline migration.'
            )
            offline = True

        if not offline and run_puppet:
            raise IGVMError('Online migration cannot run Puppet.')

        # Validate destination hypervisor can run the VM (needs to happen after
        # setting new IP!)
        hypervisor.check_vm(_vm, offline)

        # After the HV is chosen, disk_size_gib must be restored
        # to pass _check_attributes(_vm)
        _vm.dataset_obj['disk_size_gib'] = original_size_gib

        # Require VM to be in sync with serveradmin
        _check_attributes(_vm)
        _vm.check_serveradmin_config()

        with Transaction() as transaction:
            _vm.hypervisor.migrate_vm(
                _vm, hypervisor, offline, offline_transport, transaction,
                no_shutdown, disk_size,
            )
            previous_hypervisor = _vm.hypervisor
            _vm.hypervisor = hypervisor

            def _reset_hypervisor():
                _vm.hypervisor = previous_hypervisor

            transaction.on_rollback('reset hypervisor', _reset_hypervisor)

            if run_puppet:
                hypervisor.mount_vm_storage(_vm, transaction)
                _vm.run_puppet(debug=debug_puppet)
                hypervisor.umount_vm_storage(_vm)

            if offline and was_running:
                _vm.start(transaction=transaction)

            _vm.reset_state()

            # Add migration log entries to hypervisor and previous_hypervisor
            hypervisor.log_migration(_vm, '+')
            transaction.on_rollback(
                'reset hypervisor log',
                hypervisor.log_migration,
                _vm,
                '-',
            )

            previous_hypervisor.log_migration(_vm, '-')
            transaction.on_rollback(
                'reset previous hypervisor log',
                previous_hypervisor.log_migration,
                _vm,
                '+',
            )

            # Update Serveradmin
            _vm.dataset_obj['hypervisor'] = hypervisor.dataset_obj['hostname']
            _vm.dataset_obj.commit()

        # If removing the existing VM fails we shouldn't risk undoing the newly
        # migrated one.
        previous_hypervisor.undefine_vm(_vm)


@with_fabric_settings
def vm_start(vm_hostname, unretire=None):
    """Start a VM"""
    with _get_vm(vm_hostname) as vm:
        if unretire and vm.dataset_obj['state'] != 'retired':
            raise InvalidStateError('Can\'t unretire a non-retired VM!')

        if vm.dataset_obj['datacenter_type'] == 'aws.dct':
            vm.aws_start()
        elif vm.dataset_obj['datacenter_type'] == 'kvm.dct':
            _check_defined(vm)
            if vm.is_running():
                log.info('"{}" is already running.'.format(vm.fqdn))
                return
            vm.start()
        else:
            raise NotImplementedError(
                'This operation is not yet supported for {}'.format(
                    vm.dataset_obj['datacenter_type'])
            )

        if unretire:
            vm.dataset_obj['state'] = unretire
            vm.dataset_obj.commit()


@with_fabric_settings
def vm_stop(vm_hostname, force=False, retire=False):
    """Gracefully stop a VM"""
    with _get_vm(vm_hostname, allow_retired=True) as vm:
        if vm.dataset_obj['datacenter_type'] == 'aws.dct':
            vm.aws_shutdown()
        elif vm.dataset_obj['datacenter_type'] == 'kvm.dct':
            _check_defined(vm)

            if not vm.is_running():
                log.info('"{}" is already stopped.'.format(vm.fqdn))
                return
            if force:
                vm.hypervisor.stop_vm_force(vm)
            else:
                vm.shutdown()
        else:
            raise NotImplementedError(
                'This operation is not yet supported for {}'.format(
                    vm.dataset_obj['datacenter_type'])
            )

        if retire:
            vm.dataset_obj['state'] = 'retired'
            vm.dataset_obj.commit()
            log.info('"{}" is retired.'.format(vm.fqdn))

        log.info('"{}" is stopped.'.format(vm.fqdn))


@with_fabric_settings
def vm_restart(vm_hostname, force=False, no_redefine=False):
    """Restart a VM

    The VM is shut down and recreated, using the existing disk. This can be
    useful to discard temporary changes or adapt new hypervisor optimizations.
    No data will be lost.
    """
    with ExitStack() as es:
        vm = es.enter_context(_get_vm(vm_hostname))
        if vm.dataset_obj['datacenter_type'] == 'aws.dct':
            vm.aws_shutdown()
            vm.aws_start()
        elif vm.dataset_obj['datacenter_type'] == 'kvm.dct':
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
        else:
            raise NotImplementedError(
                'This operation is not yet supported for {}'.format(
                    vm.dataset_obj['datacenter_type'])
            )

        log.info('"{}" is restarted.'.format(vm.fqdn))


@with_fabric_settings
def vm_delete(vm_hostname, retire=False):
    """Delete the VM from the hypervisor and from serveradmin

    If retire is True the VM will not be deleted from serveradmin but it's
    state will be updated to 'retired'.
    """

    with _get_vm(vm_hostname, unlock=retire, allow_retired=True) as vm:
        if vm.dataset_obj['datacenter_type'] == 'aws.dct':
            vm_status_code = vm.aws_describe_instance_status(
                vm.dataset_obj['aws_instance_id'])
            if vm_status_code != AWS_RETURN_CODES['stopped']:
                raise InvalidStateError(
                    '"{}" is still running.'.format(vm.fqdn))
            else:
                vm.aws_delete()
        elif vm.dataset_obj['datacenter_type'] == 'kvm.dct':
            # Make sure the VM has a hypervisor and that it is defined on it.
            # Abort if the VM has not been defined.
            _check_defined(vm)

            # Make sure the VM is shut down, abort if it is not.
            if (
                vm.hypervisor
                and vm.hypervisor.vm_defined(vm)
                and vm.is_running()
            ):
                raise InvalidStateError('"{}" is still running.'.format(
                    vm.fqdn)
                )

            # Delete the VM from its hypervisor if required.
            if vm.hypervisor and vm.hypervisor.vm_defined(vm):
                vm.hypervisor.undefine_vm(vm)
        else:
            raise NotImplementedError(
                'This operation is not yet supported for {}'.format(
                    vm.dataset_obj['datacenter_type'])
            )

        # Delete the machines cert from puppet in case we want to build
        # one with the same name in the future
        puppet.clean_cert(vm.dataset_obj)

        # Delete the serveradmin object of this VM
        # or update its state to 'retired' if retire is True.
        if retire:
            vm.dataset_obj['state'] = 'retired'
            # We must clean the hypervisor attribute, as we enforce that no
            # hypervisor has retired VMs assigned to it.
            vm.dataset_obj['hypervisor'] = None
            vm.dataset_obj.commit()
            log.info(
                '"{}" is destroyed and set to "retired" state.'.format(
                    vm.fqdn)
            )
        else:
            vm.dataset_obj.delete()
            vm.dataset_obj.commit()
            log.info(
                '"{}" is destroyed and deleted from Serveradmin'.format(
                    vm.fqdn)
            )


@with_fabric_settings
def vm_sync(vm_hostname):
    """Synchronize VM resource attributes to Serveradmin

    This command collects actual resource allocation of a VM from the
    hypervisor and overwrites outdated attribute values in Serveradmin."""
    with _get_vm(vm_hostname) as vm:
        if vm.dataset_obj['datacenter_type'] == 'aws.dct':
            attributes = vm.aws_sync()
        elif vm.dataset_obj['datacenter_type'] == 'kvm.dct':
            _check_defined(vm)
            attributes = vm.hypervisor.vm_sync_from_hypervisor(vm)
        else:
            raise NotImplementedError(
                'This operation is not yet supported for {}'.format(
                    vm.dataset_obj['datacenter_type'])
            )

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
                '"{}" is synchronized {} attributes ({}).'.format(
                    vm.fqdn, len(changed), ', '.join(changed))
            )
        else:
            log.info(
                '"{}" is already synchronized on Serveradmin.'.format(vm.fqdn)
            )


def vm_define(vm_hostname):
    """Define VM on hypervisor

    This command executes necessary code to just define the VM aka create the
    domain.xml for libvirt. It is a convenience command to restore a domain
    in case you lost your SSH session while the domain was not defined.

    :param: vm_hostname: hostname of VM
    """

    vm_dataset_obj = Query({'hostname': vm_hostname}, VM_ATTRIBUTES).get()
    hv = Hypervisor(vm_dataset_obj['hypervisor'])
    vm = VM(vm_dataset_obj, hv)

    hv.define_vm(vm)
    vm.start()

    log.info('VM {} defined and booted on {}'.format(
        vm_hostname, vm_dataset_obj['hypervisor']['hostname']))


@with_fabric_settings  # NOQA: C901
def host_info(vm_hostname):
    """Extract runtime information about a VM

    Library consumers should use VM.info() directly.
    """
    with _get_vm(vm_hostname) as vm:

        if vm.dataset_obj['datacenter_type'] != 'kvm.dct':
            raise NotImplementedError(
                'This operation is not yet supported for {}'.format(
                    vm.dataset_obj['datacenter_type'])
            )

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
                'Max:     {} {unit}'.format(
                    capacity - free, free, capacity, unit=unit))

            if not 0 <= free <= capacity > 0:
                log.warning(
                    '{} ({}) and {} ({}) have weird ratio, skipping progress '
                    'calculation'.format(
                        free_key, free, capacity_key, capacity)
                )
                info[result_key] = red(simple_stats)
                return

            assert 0 <= free <= capacity
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
                '[{}{}] {}%\n{}'.format(
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

    with _get_vm(vm_hostname) as vm:
        if vm.dataset_obj['datacenter_type'] not in ['aws.dct', 'kvm.dct']:
            raise NotImplementedError(
                'This operation is not yet supported for {}'.format(
                    vm.dataset_obj['datacenter_type']
                )
            )

        if vm.dataset_obj['puppet_disabled']:
            raise ConfigError(
                'Rename command only works with Puppet enabled'
            )

        if vm.dataset_obj['datacenter_type'] == 'kvm.dct':
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
        elif vm.dataset_obj['datacenter_type'] == 'aws.dct':
            vm.aws_rename(new_hostname)


@with_fabric_settings
def clean_cert(hostname: str):
    """Revoke and delete a Puppet certificate from the Puppet CA"""
    vm = Query({'hostname': hostname}, ['hostname', 'puppet_ca']).get()
    puppet.clean_cert(vm)


@contextmanager
def _get_vm(hostname, unlock=True, allow_retired=False):
    """Get a server from Serveradmin by hostname to return VM object

    The function is accepting hostnames in any length as long as it resolves
    to a single server on Serveradmin.
    """

    object_id = Query({
        'hostname': Any(hostname, StartsWith(hostname + '.')),
        'servertype': 'vm',
    }, ['object_id']).get()['object_id']

    def vm_query():
        return Query({
            'object_id': object_id,
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
        if not allow_retired and dataset_obj['state'] == 'retired':
            raise InvalidStateError(
                'VM {} is in state retired, I refuse to work on it!'.format(
                    hostname,
                )
            )
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
def _get_hypervisor(hostname, allow_reserved=False):
    """Get a server from Serveradmin by hostname to return Hypervisor object"""
    dataset_obj = Query({
        'hostname': hostname,
        'servertype': 'hypervisor',
    }, HYPERVISOR_ATTRIBUTES).get()

    if not allow_reserved and dataset_obj['state'] == 'online_reserved':
        raise InvalidStateError(
            'Server "{0}" is online_reserved.'.format(dataset_obj['hostname'])
        )

    hypervisor = Hypervisor(dataset_obj)
    hypervisor.acquire_lock()

    try:
        yield hypervisor
    finally:
        hypervisor.release_lock()


@contextmanager
def _get_best_hypervisor(
    vm,
    hypervisor_states,
    offline=False,
    enforce_vm_env=False,
    soft_preferences=False,
    additional_filter=None,
):
    hv_filter = {
        'servertype': 'hypervisor',
        'vlan_networks': vm.route_network,
        'state': Any(*hypervisor_states),
    }

    # Enforce IGVM_MODE used for tests
    if 'IGVM_MODE' in environ:
        hv_filter['environment'] = environ.get('IGVM_MODE')
    else:
        if enforce_vm_env:
            hv_filter['environment'] = vm.dataset_obj['environment']

    # Merge additional filter, if any
    additional_filter = additional_filter or {}
    for k, v in additional_filter.items():
        if k in hv_filter and v != hv_filter[k]:
            raise InvalidStateError(
                f'Requested {k}={str(v)}, '
                f'but "{k}" is already set to "{str(hv_filter[k])}"',
            )
        elif k in hv_filter:
            continue

        hv_filter[k] = v

    # Get all (theoretically) possible HVs sorted by HV preferences
    hypervisors = (
        Hypervisor(o) for o in
        Query(hv_filter, HYPERVISOR_ATTRIBUTES)
    )
    hypervisors = sort_by_preference(
        vm,
        HYPERVISOR_PREFERENCES,
        hypervisors,
        soft_preferences,
    )

    possible_hvs = OrderedDict()
    for possible_hv in hypervisors:
        possible_hvs[str(possible_hv)] = possible_hv

    # Check all HVs in parallel. This will check live data on those HVs
    # but without locking them. This allows us to do a real quick first
    # filtering round. Below follows another one on the filtered HVs only.
    chunk_size = 10
    iterations = math.ceil(len(possible_hvs) / chunk_size)
    found_hv = None

    # We are checking HVs in chunks. This will enable us to select HVs early
    # without looping through all of them if unnecessary.
    for i in range(iterations):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size
        hv_chunk = dict(list(possible_hvs.items())[start_idx:end_idx])

        results = parallel(
            _check_vm,
            identifiers=list(hv_chunk.keys()),
            args=[
                [possible_hv, vm, offline]
                for possible_hv in hv_chunk.values()
            ],
            workers=chunk_size,
        )

        # Remove unsupported HVs from the list
        for checked_hv, success in results.items():
            if not success:
                hv_chunk.pop(checked_hv)

        # Do another checking iteration, this time with HV locking
        for possible_hv in hv_chunk.values():
            try:
                possible_hv.acquire_lock()
            except InvalidStateError as e:
                log.warning(e)
                continue

            if not _check_vm(possible_hv, vm, offline):
                possible_hv.release_lock()
                continue

            # HV found
            found_hv = possible_hv

            break

        if found_hv:
            break

    if not found_hv:
        # No supported HV was found
        raise IGVMError(
            'Automatically finding the best Hypervisor failed! '
            'Can not find a suitable hypervisor with the preferences and '
            'the Query: {}'.format(hv_filter))

    # Yield the hypervisor locked for working on it
    try:
        log.info('Picked {} as destination Hypervisor'.format(str(found_hv)))
        yield found_hv
    finally:
        found_hv.release_lock()


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


def _check_vm(hv, vm, offline):
    try:
        hv.check_vm(vm, offline)

        return True
    except (libvirtError, HypervisorError) as e:
        log.warning(
            'Preferred hypervisor "{}" is skipped: {}'.format(hv, e)
        )

        return False
