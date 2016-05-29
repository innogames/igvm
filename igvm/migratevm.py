import copy

from fabric.api import env, execute, run

from adminapi import api

from igvm.hypervisor import Hypervisor
from igvm.utils.resources import get_hw_model
from igvm.utils import ManageVMError
from igvm.utils.config import (
        get_server,
        check_dsthv_memory,
        check_dsthv_cpu,
        import_vm_config_from_admintool,
        check_vm_config,
        import_vm_config_from_xen,
        import_vm_config_from_kvm,
    )
from igvm.utils.preparevm import run_puppet
from igvm.utils.storage import (
        get_vm_block_dev,
        netcat_to_device,
        device_to_netcat,
        StorageError,
    )
from igvm.utils.virtutils import (
        get_virtconn,
        close_virtconns,
    )
from igvm.vm import VM

# Configuration of Fabric:
env.disable_known_hosts = True
env.use_ssh_config = True
env.always_use_pty = False
env.forward_agent = True
env.user = 'root'
env.shell = '/bin/bash -c'


def setup_dsthv(config, offline):
    vm = config['vm_object']
    dsthv = config['dsthv_object']

    check_dsthv_cpu(config)
    check_dsthv_memory(config)

    config['vm_block_dev'] = get_vm_block_dev(config['dsthv']['hypervisor'])
    config['dst_device'] = dsthv.create_vm_storage(vm)

    if offline:
        config['nc_port'] = netcat_to_device(config['dst_device'])


def add_dsthv_to_ssh(config):
    run('touch .ssh/known_hosts'.format(config['dsthv_hostname']))
    run('ssh-keygen -R {0}'.format(config['dsthv_hostname']))
    run('ssh-keyscan -t rsa {0} >> .ssh/known_hosts'.format(config['dsthv_hostname']))


def migrate_offline(config):
    add_dsthv_to_ssh(config)
    execute(
        device_to_netcat,
        config['src_device'],
        config['vm']['disk_size_gib'] * 1024**3,
        config['dsthv_hostname'],
        config['nc_port'],
        hosts=config['srchv']['hostname'],
    )


def start_offline_vm(config):
    hv = Hypervisor.get(config['dsthv']['hostname'])
    vm = VM(config['vm_hostname'], hv)

    if config['runpuppet']:
        vm_path = hv.mount_vm_storage(vm)
        run_puppet(vm_path, config['vm_hostname'], False)
        hv.umount_vm_storage(vm)

    config['dsthv_hw_model'] = get_hw_model(config['dsthv'])

    # We distinguish between src_device and dst_device, which create() doesn't know about.
    create_config = copy.copy(config)
    create_config['device'] = config['dst_device']

    vm.create(create_config)

    vm.start()


def migrate_virsh(config):

    # Unfortunately, virsh provides a global timeout, but what we need it to
    # timeout if it is catching up the dirtied memory.  To be in this stage,
    # it should have coped the initial disk and memory and changes on them.
    timeout = sum((
            # We assume the disk can be copied at 50 MB/s;
            config['vm']['disk_size_gib'] * 1024 / 50,
            # the memory at 100 MB/s;
            config['mem'] / 100,
            # and 5 minutes more for other operations.
            5 * 60,
        ))

    migrate_cmd = ('virsh migrate'
            ' --live'               # Do it live!
            ' --copy-storage-all'
            ' --persistent'         # Define the VM on the new host
            ' --change-protection'  # Don't let the VM configuration to be changed
            ' --auto-converge'      # Force convergence, otherwise migrations never end
            ' --domain {vm_hostname}'
            ' --abort-on-error'     # Don't tolerate soft errors
            ' --desturi qemu+ssh://{dsthv_hostname}/system' # We need SSH agent forwarding
            ' --timeout {timeout}'  # Force guest to suspend, if noting else helped
            ' --verbose'
        )

    add_dsthv_to_ssh(config)
    run(migrate_cmd.format(
        vm_hostname    = config['vm_hostname'],
        dsthv_hostname = config['dsthv_hostname'],
        timeout        = timeout,
    ))


def _migratevm(config, newip, nolbdowntime, offline):
    config['vm'] = get_server(config['vm_hostname'], 'vm')

    # TODO We are not validating the servertype of the source and target
    # hypervisor for now, because of the old hypervisors with servertype
    # "db_server" and "frontend_server".  Fix this after the migration is
    # complete.
    config['srchv'] = get_server(config['vm']['xen_host'])
    config['dsthv'] = get_server(config['dsthv_hostname'])

    if config['dsthv']['state'] != 'online':
        raise Exception('Server "{0}" is not online.'.format(config['dsthv']['hostname']))

    source_hv = Hypervisor.get(config['srchv'])
    destination_hv = Hypervisor.get(config['dsthv'])
    source_vm = VM(config['vm'], source_hv)
    config['vm_object'] = source_vm
    config['dsthv_object'] = destination_hv

    # There is no point of online migration, if the VM is already
    # shutdown.
    if not offline and not source_vm.is_running():
        offline = True

    if not offline and newip:
        raise ManageVMError('Online migration cannot change IP address.')

    source_hv.check_migration(source_vm, destination_hv, offline)

    lb_api = api.get('lbadmin')
    downtime_network = None

    if not nolbdowntime and 'testtool_downtime' in config['vm']:
        if config['vm']['segment'] in ['af', 'aw', 'vn', 'none']:
            network_api = api.get('ip')
            for iprange in network_api.get_matching_ranges(config['vm']['intern_ip']):
                if iprange['belongs_to'] == None and iprange['type'] == 'private':
                    if downtime_network:
                        raise ManageVMError('Unable to determine network for testtool downtime. Multiple networks found.')
                    downtime_network = iprange['range_id']
        else:
            downtime_network = config['vm']['segment']
        if not downtime_network:
            raise ManageVMError('Unable to determine network for testtool downtime. No network found.')

    if newip:
        source_vm._set_ip(newip)

    # Validate dst HV can run VM (needs to happen after setting new IP!)
    destination_hv.check_vm(source_vm)

    # First, get the VM information from the Serveradmin.  The next
    # step should validate that information.
    import_vm_config_from_admintool(config)

    # Import information about VM from source Hypervisor
    if config['srchv']['hypervisor'] == 'xen':
        execute(import_vm_config_from_xen, source_vm, config, hosts=[config['srchv']['hostname']])
    elif config['srchv']['hypervisor'] == 'kvm':
        config['srchv_conn'] = get_virtconn(config['srchv']['hostname'], 'kvm')
        execute(import_vm_config_from_kvm, source_vm, config, hosts=[config['srchv']['hostname']])
    else:
        raise ManageVMError("Migration from Hypervisor type {0} is not supported".format(config['srchv']['hypervisor']))

    # Verify if config contains all the needed parameters
    check_vm_config(config)

    # Setup destination Hypervisor
    if config['dsthv']['hypervisor'] == 'xen':
        execute(setup_dsthv, config, offline, hosts=[config['dsthv']['hostname']])
    elif config['dsthv']['hypervisor'] == 'kvm':
        config['dsthv_conn'] = get_virtconn(config['dsthv']['hostname'], 'kvm')
        execute(setup_dsthv, config, offline, hosts=[config['dsthv']['hostname']])
    else:
        raise ManageVMError("Migration to Hypervisor type {0} is not supported".format(config['dsthv']['hypervisor']))

    # Commit previously changed IP address and segment.
    if newip:
        config['vm'].commit()

    if not nolbdowntime and 'testtool_downtime' in config['vm']:
        print "Downtiming testtool for network '{}'".format(downtime_network)
        config['vm']['testtool_downtime'] = True
        config['vm'].commit()
        try:
            lb_api.push_downtimes([downtime_network])
        except:
            pass
    # Finally migrate the VM
    if offline:
        if source_vm.is_running():
            source_vm.shutdown()
        execute(migrate_offline, config, hosts=[config['srchv']['hostname']])
        execute(start_offline_vm, config, hosts=[config['dsthv']['hostname']])
    else:
        execute(migrate_virsh, config, hosts=[config['srchv']['hostname']])

    if not nolbdowntime and 'testtool_downtime' in config['vm']:
        print "Removing testtool downtime"
        config['vm']['testtool_downtime'] = False
        config['vm'].commit()
        try:
            lb_api.push_downtimes([downtime_network])
        except:
            pass

    # Update admintool information
    config['vm']['xen_host'] = config['dsthv']['hostname']
    config['vm']['num_cpu'] = config['num_cpu']
    config['vm']['memory'] = config['mem']
    config['vm'].commit()

    # Remove the existing VM
    source_vm.undefine()

    source_hv.destroy_vm_storage(source_vm)


def migratevm(vm_hostname, dsthv_hostname, newip=None, nopuppet=False, nolbdowntime=False, offline=False):
    config = {
        'vm_hostname': vm_hostname,
        'dsthv_hostname': dsthv_hostname,
        'runpuppet': not nopuppet,
    }

    try:
        _migratevm(config, newip, nolbdowntime, offline)
    except StorageError as e:
        # TODO: Perform cleanup
        raise ManageVMError(e)
    else:
        pass
    finally:
        close_virtconns()
