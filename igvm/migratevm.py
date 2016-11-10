import logging

from igvm.exceptions import IGVMError, InconsistentAttributeError
from igvm.host import with_fabric_settings
from igvm.hypervisor import Hypervisor
from igvm.utils.preparevm import run_puppet
from igvm.utils.storage import (
    netcat_to_device,
    device_to_netcat,
)
from igvm.utils.transaction import run_in_transaction
from igvm.vm import VM

log = logging.getLogger(__name__)


@with_fabric_settings
@run_in_transaction
def migratevm(vm_hostname, dsthv_hostname, newip=None, runpuppet=False,
               maintenance=False, offline=False, tx=None, ignore_reserved=False):
    """Migrate a VM to a new hypervisor."""
    assert tx is not None, 'tx populated by run_in_transaction'

    vm = VM(vm_hostname)
    source_hv = vm.hypervisor
    destination_hv = Hypervisor.get(dsthv_hostname, ignore_reserved)

    # There is no point of online migration, if the VM is already
    # shutdown.
    if not offline and not vm.is_running():
        offline = True

    if not offline and newip:
        raise IGVMError('Online migration cannot change IP address.')

    if not offline and runpuppet:
        raise IGVMError('Online migration cannot run Puppet.')

    if not runpuppet and newip:
        raise IGVMError(
            'Changing IP requires a Puppet run, pass --runpuppet.'
        )

    # Require VM to be in sync with serveradmin
    synced_attributes = source_hv.vm_sync_from_hypervisor(vm)
    for attr, value in synced_attributes.iteritems():
        if vm.admintool[attr] != value:
            raise InconsistentAttributeError(vm, attr, value)

    vm.check_serveradmin_config()
    source_hv.check_migration(vm, destination_hv, offline)

    if newip:
        vm._set_ip(newip)

    # Validate dst HV can run VM (needs to happen after setting new IP!)
    destination_hv.check_vm(vm)

    # Setup destination Hypervisor
    dst_device = destination_hv.create_vm_storage(vm, tx)
    if offline:
        maintenance = True
        nc_listener = netcat_to_device(destination_hv, dst_device, tx)

    # Commit previously changed IP address.
    if newip:
        # TODO: This commit is not rolled back.
        vm.admintool.commit()
        tx.on_rollback('newip warning', log.info, '--newip is not rolled back')

    if maintenance:
        vm.set_state('maintenance', tx=tx)

    # Finally migrate the VM
    if offline:
        if vm.is_running():
            vm.shutdown(tx=tx)

        source_hv.accept_ssh_hostkey(destination_hv)
        device_to_netcat(
            source_hv,
            source_hv.vm_disk_path(vm),
            vm.admintool['disk_size_gib'] * 1024**3,
            nc_listener,
            tx,
        )

        if runpuppet:
            destination_hv.mount_vm_storage(vm, tx)
            run_puppet(destination_hv, vm, clear_cert=False, tx=tx)
            destination_hv.umount_vm_storage(vm)

        destination_hv.define_vm(vm, tx)
        vm.hypervisor = destination_hv

        def _reset_hypervisor():
            vm.hypervisor = source_hv
        tx.on_rollback('reset hypervisor', _reset_hypervisor)

        vm.start(tx=tx)
    else:
        source_hv.vm_migrate_online(vm, destination_hv)
        vm.hypervisor = destination_hv

    vm.reset_state()

    # Update admintool information
    vm.admintool['xen_host'] = destination_hv.hostname
    vm.admintool.commit()

    # If removing the existing VM fails we shouldn't risk undoing the newly
    # migrated one.
    tx.checkpoint()

    # Remove the existing VM
    source_hv.undefine_vm(vm)
    source_hv.destroy_vm_storage(vm)
