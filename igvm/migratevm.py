import logging

from fabric.api import settings

from adminapi import api

from igvm.exceptions import IGVMError
from igvm.hypervisor import Hypervisor
from igvm.settings import COMMON_FABRIC_SETTINGS
from igvm.utils.preparevm import run_puppet
from igvm.utils.storage import (
    netcat_to_device,
    device_to_netcat,
)
from igvm.vm import VM

log = logging.getLogger(__name__)


def add_dsthv_to_ssh(host, dst_host):
    host.run('touch .ssh/known_hosts'.format(dst_host.hostname))
    host.run('ssh-keygen -R {0}'.format(dst_host.hostname))
    host.run(
        'ssh-keyscan -t rsa {0} >> .ssh/known_hosts'
        .format(dst_host.hostname)
    )


def migrate_virsh(source_hv, destination_hv, vm):

    # Unfortunately, virsh provides a global timeout, but what we need it to
    # timeout if it is catching up the dirtied memory.  To be in this stage,
    # it should have coped the initial disk and memory and changes on them.
    timeout = sum((
        # We assume the disk can be copied at 50 MB/s;
        vm.admintool['disk_size_gib'] * 1024 / 50,
        # the memory at 100 MB/s;
        vm.admintool['memory'] / 100,
        # and 5 minutes more for other operations.
        5 * 60,
    ))

    migrate_cmd = (
        'virsh migrate'
        # Do it live!
        ' --live'
        ' --copy-storage-all'
        # Define the VM on the new host
        ' --persistent'
        # Don't let the VM configuration to be changed
        ' --change-protection'
        # Force convergence, # otherwise migrations never end
        ' --auto-converge'
        ' --domain {vm_hostname}'
        # Don't tolerate soft errors
        ' --abort-on-error'
        # We need SSH agent forwarding
        ' --desturi qemu+ssh://{dsthv_hostname}/system'
        # Force guest to suspend, if noting else helped
        ' --timeout {timeout}'
        ' --verbose'
    )

    add_dsthv_to_ssh(source_hv, destination_hv)
    source_hv.run(migrate_cmd.format(
        vm_hostname=vm.hostname,
        dsthv_hostname=destination_hv.hostname,
        timeout=timeout,
    ))


def migratevm(*args, **kwargs):
    with settings(**COMMON_FABRIC_SETTINGS):
        _migratevm(*args, **kwargs)


def _migratevm(vm_hostname, dsthv_hostname, newip=None, runpuppet=False,
               nolbdowntime=False, offline=False):
    vm = VM(vm_hostname)
    source_hv = vm.hypervisor
    destination_hv = Hypervisor.get(dsthv_hostname)

    # There is no point of online migration, if the VM is already
    # shutdown.
    if not offline and not vm.is_running():
        offline = True

    if not offline and newip:
        raise IGVMError('Online migration cannot change IP address.')

    if not runpuppet and newip:
        raise IGVMError(
            'Changing IP requires a Puppet run, pass --runpuppet.'
        )

    # Require VM to be in sync with serveradmin
    synced_attributes = {}
    source_hv.vm_sync_from_hypervisor(vm, synced_attributes)
    for attr, value in synced_attributes.iteritems():
        if vm.admintool[attr] != value:
            raise IGVMError(
                'Attribute "{}" is out of sync: {} (config) != {} (actual)'
                .format(attr, vm.admintool[attr], value)
            )

    vm.check_serveradmin_config()
    source_hv.check_migration(vm, destination_hv, offline)

    lb_api = api.get('lbadmin')
    downtime_network = None

    # TODO: Use state
    if not nolbdowntime and 'testtool_downtime' in vm.admintool:
        if vm.admintool['segment'] in ['af', 'aw', 'vn', 'none']:
            network_api = api.get('ip')
            for iprange in network_api.get_matching_ranges(vm.admintool['intern_ip']):
                if iprange['belongs_to'] == None and iprange['type'] == 'private':
                    if downtime_network:
                        raise IGVMError('Unable to determine network for testtool downtime. Multiple networks found.')
                    downtime_network = iprange['range_id']
        else:
            downtime_network = vm.admintool['segment']
        if not downtime_network:
            raise IGVMError('Unable to determine network for testtool downtime. No network found.')

    if newip:
        vm._set_ip(newip)

    # Validate dst HV can run VM (needs to happen after setting new IP!)
    destination_hv.check_vm(vm)

    # Setup destination Hypervisor
    dst_device = destination_hv.create_vm_storage(vm)
    if offline:
        nc_listener = netcat_to_device(destination_hv, dst_device)

    # Commit previously changed IP address and segment.
    if newip:
        vm.admintool.commit()

    if not nolbdowntime and 'testtool_downtime' in vm.admintool:
        print "Downtiming testtool for network '{}'".format(downtime_network)
        vm.admintool['testtool_downtime'] = True
        vm.admintool.commit()
        try:
            lb_api.push_downtimes([downtime_network])
        except:
            pass

    # Finally migrate the VM
    if offline:
        if vm.is_running():
            vm.shutdown()

        add_dsthv_to_ssh(source_hv, destination_hv)
        device_to_netcat(
            source_hv,
            source_hv.vm_disk_path(vm),
            vm.admintool['disk_size_gib'] * 1024**3,
            nc_listener,
        )

        if runpuppet:
            destination_hv.mount_vm_storage(vm)
            run_puppet(destination_hv, vm, clear_cert=False)
            destination_hv.umount_vm_storage(vm)

        destination_hv.define_vm(vm)
        vm.hypervisor = destination_hv
        vm.start()
    else:
        migrate_virsh(source_hv, destination_hv, vm)
        vm.hypervisor = destination_hv

    # TODO: Use state
    if not nolbdowntime and 'testtool_downtime' in vm.admintool:
        log.info("Removing testtool downtime")
        vm.admintool['testtool_downtime'] = False
        vm.admintool.commit()
        try:
            lb_api.push_downtimes([downtime_network])
        except:
            pass

    # Update admintool information
    vm.admintool['xen_host'] = destination_hv.hostname
    vm.admintool.commit()

    # Remove the existing VM
    source_hv.undefine_vm(vm)
    source_hv.destroy_vm_storage(vm)
