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
              maintenance=False, offline=False, tx=None,
              ignore_reserved=False):
    """Migrate a VM to a new hypervisor."""
    assert tx is not None, 'tx populated by run_in_transaction'

    # For source HV we ignore reserved flag.
    # It must always be possible to move VMs out of a HV
    vm = VM(vm_hostname, ignore_reserved=True)
    destination_hv = Hypervisor.get(dsthv_hostname, ignore_reserved)

    # There is no point of online migration, if the VM is already shutdown.
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
    check_attributes(vm)

    vm.check_serveradmin_config()
    vm.hypervisor.check_migration(vm, destination_hv, offline)

    if newip:
        vm._set_ip(newip)

    # Validate dst HV can run VM (needs to happen after setting new IP!)
    destination_hv.check_vm(vm)

    # Setup destination Hypervisor
    dst_device = destination_hv.create_vm_storage(vm, tx)

    # Commit previously changed IP address.
    if newip:
        # TODO: This commit is not rolled back.
        vm.server_obj.commit()
        tx.on_rollback('newip warning', log.info, '--newip is not rolled back')

    if maintenance or offline:
        vm.set_state('maintenance', tx=tx)

    # Finally migrate the VM
    if offline:
        offline_migrate(vm, destination_hv, dst_device, runpuppet, tx)
    else:
        vm.hypervisor.vm_migrate_online(vm, destination_hv)
        vm.hypervisor = destination_hv

    vm.reset_state()

    # Update Serveradmin
    vm.server_obj['xen_host'] = destination_hv.hostname
    vm.server_obj.commit()

    # If removing the existing VM fails we shouldn't risk undoing the newly
    # migrated one.
    tx.checkpoint()

    # Remove the existing VM
    vm.hypervisor.undefine_vm(vm)
    vm.hypervisor.destroy_vm_storage(vm)


def check_attributes(vm):
    synced_attributes = vm.hypervisor.vm_sync_from_hypervisor(vm)
    for attr, value in synced_attributes.iteritems():
        if vm.server_obj[attr] != value:
            raise InconsistentAttributeError(vm, attr, value)


def offline_migrate(vm, destination_hv, dst_device, runpuppet, tx):
    source_hv = vm.hypervisor
    nc_listener = netcat_to_device(destination_hv, dst_device, tx)
    if vm.is_running():
        vm.shutdown(tx=tx)

    source_hv.accept_ssh_hostkey(destination_hv)
    device_to_netcat(
        source_hv,
        source_hv.vm_disk_path(vm),
        vm.server_obj['disk_size_gib'] * 1024**3,
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
