"""igvm - migratevm

Copyright (c) 2018, InnoGames GmbH
"""

import logging

from igvm.exceptions import IGVMError, InconsistentAttributeError
from igvm.host import with_fabric_settings
from igvm.hypervisor import Hypervisor
from igvm.settings import MIGRATE_COMMANDS
from igvm.transaction import Transaction
from igvm.vm import VM

log = logging.getLogger(__name__)


@with_fabric_settings   # NOQA: C901
def migratevm(vm_hostname, hypervisor_hostname=None, newip=None,
              runpuppet=False, maintenance=False, offline=False,
              ignore_reserved=False):
    """Migrate a VM to a new hypervisor."""
    vm = VM(vm_hostname, ignore_reserved=ignore_reserved)

    # If not specified automatically find a new better hypervisor
    if hypervisor_hostname:
        hypervisor = Hypervisor(
            hypervisor_hostname, ignore_reserved=ignore_reserved
        )
    else:
        hypervisor = vm.get_best_hypervisor(
            ['online', 'online_reserved'] if ignore_reserved else ['online']
        )

    was_running = vm.is_running()

    # There is no point of online migration, if the VM is already shutdown.
    if not was_running:
        offline = True

    if not offline and newip:
        raise IGVMError('Online migration cannot change IP address.')

    if not offline and runpuppet:
        raise IGVMError('Online migration cannot run Puppet.')

    if not runpuppet and newip:
        raise IGVMError(
            'Changing IP requires a Puppet run, pass --runpuppet.'
        )

    source_hv_os = vm.hypervisor.dataset_obj['os']
    destination_hv_os = hypervisor.dataset_obj['os']
    if (
        not offline and
        (source_hv_os, destination_hv_os) not in MIGRATE_COMMANDS
    ):
        raise IGVMError(
            'Online migration from {} to {} is not supported!'.format(
                source_hv_os,
                destination_hv_os,
            )
        )

    # Require VM to be in sync with serveradmin
    check_attributes(vm)

    vm.check_serveradmin_config()
    vm.hypervisor.check_migration(vm, hypervisor, offline)

    if newip:
        vm._set_ip(newip)

    # Validate destination hypervisor can run the VM (needs to happen after
    # setting new IP!)
    hypervisor.check_vm(vm)

    with Transaction() as transaction:
        # Commit previously changed IP address.
        if newip:
            # TODO: This commit is not rolled back.
            vm.dataset_obj.commit()
            transaction.on_rollback(
                'newip warning', log.info, '--newip is not rolled back'
            )

        if maintenance or offline:
            vm.set_state('maintenance', transaction=transaction)

        # Finally migrate the VM
        if offline and was_running:
            vm.shutdown(transaction=transaction)

        vm.hypervisor.migrate_vm(vm, hypervisor, offline, transaction)

        previous_hypervisor = vm.hypervisor
        vm.hypervisor = hypervisor

        def _reset_hypervisor():
            vm.hypervisor = previous_hypervisor
        transaction.on_rollback('reset hypervisor', _reset_hypervisor)

        if runpuppet:
            hypervisor.mount_vm_storage(vm, transaction)
            vm.run_puppet(clear_cert=False, transaction=transaction)
            hypervisor.umount_vm_storage(vm)

        if offline and was_running:
            vm.start(transaction=transaction)
        vm.reset_state()

        # Update Serveradmin
        vm.dataset_obj['xen_host'] = hypervisor.dataset_obj['hostname']
        vm.dataset_obj.commit()

    # If removing the existing VM fails we shouldn't risk undoing the newly
    # migrated one.
    previous_hypervisor.delete_vm(vm)


def check_attributes(vm):
    synced_attributes = vm.hypervisor.vm_sync_from_hypervisor(vm)
    for attr, value in synced_attributes.iteritems():
        if vm.dataset_obj[attr] != value:
            raise InconsistentAttributeError(vm, attr, value)
