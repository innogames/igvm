import copy
from time import strftime, sleep

from fabric.api import env, execute, run
from fabric.context_managers import hide
from fabric.network import disconnect_all

from adminapi import api

from managevm.hooks import load_hooks
from managevm.utils.resources import get_hw_model
from managevm.signals import send_signal
from managevm.utils import fail_gracefully
from managevm.utils.config import (
        get_server,
        check_dsthv_memory,
        check_dsthv_cpu,
        import_vm_config_from_admintool,
        check_vm_config,
        import_vm_config_from_xen,
        import_vm_config_from_kvm,
    )
from managevm.utils.hypervisor import VM
from managevm.utils.network import get_vlan_info
from managevm.utils.preparevm import run_puppet
from managevm.utils.storage import (
        remove_logical_volume,
        mount_temp,
        umount_temp,
        remove_temp,
        create_storage,
        get_vm_block_dev,
        netcat_to_device,
        device_to_netcat
    )
from managevm.utils.virtutils import (
        get_virtconn,
        close_virtconns,
    )

run = fail_gracefully(run)

# Configuration of Fabric:
env.disable_known_hosts = True
env.use_ssh_config = True
env.always_use_pty = False
env.forward_agent = True
env.user = 'root'
env.shell = '/bin/bash -c'

def setup_dsthv(config, offline):
    check_dsthv_cpu(config)
    check_dsthv_memory(config)

    # Invoke hooks to populate more config fields
    send_signal('populate_config', config)

    send_signal('setup_hardware', config)
    config['vm_block_dev'] = get_vm_block_dev(config['dsthv']['hypervisor'])
    config['dst_device'] = create_storage(
        config['vm']['hostname'], config['vm']['disk_size_gib']
    )

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

    if config['runpuppet']:
        vm_path = mount_temp(config['dst_device'], config['vm_hostname'])
        run_puppet( vm_path, config['vm_hostname'], False)
        umount_temp(vm_path)
        remove_temp(vm_path)

    config['dsthv_hw_model'] = get_hw_model(config['dsthv'])

    # Signals are not used in hypervisor.py, so do not migrate this stuff there!
    # Note: Extra values used to be separated from config, but since they're currently unused
    # this shouldn't matter.
    for extra in send_signal('hypervisor_extra', config, config['dsthv']['hypervisor']):
        config.update(extra)

    vm = VM.get(config['vm_hostname'], config['dsthv']['hypervisor'], config['dsthv']['hostname'])

    # We distinguish between src_device and dst_device, which create() doesn't know about.
    create_config = copy.copy(config)
    create_config['device'] = config['dst_device']

    vm.create(create_config)

    send_signal('defined_vm', config, config['dsthv']['hypervisor'])

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

def migratevm(vm_hostname, dsthv_hostname, newip=None, nopuppet=False, nolbdowntime=False, offline=False):
    load_hooks()

    config = {
        'vm_hostname': vm_hostname,
        'dsthv_hostname': dsthv_hostname,
        'runpuppet': not nopuppet,
    }

    config['vm'] = get_server(vm_hostname, 'vm')

    # TODO We are not validating the servertype of the source and target
    # hypervisor for now, because of the old hypervisors with servertype
    # "db_server" and "frontend_server".  Fix this after the migration is
    # complete.
    config['srchv'] = get_server(config['vm']['xen_host'])
    config['dsthv'] = get_server(dsthv_hostname)

    if config['dsthv']['state'] != 'online':
        raise Exception('Server "{0}" is not online.'.format(config['dsthv']['hostname']))

    source_vm = VM.get(
        vm_hostname,
        config['srchv']['hypervisor'],
        config['srchv']['hostname'],
    )

    # There is no point of online migration, if the VM is already
    # shutdown.
    if not offline and not source_vm.is_running():
        offline = True

    lb_api = api.get('lbadmin')

    if config['srchv']['hostname'] == config['dsthv']['hostname']:
        raise Exception("Source and destination Hypervisor is the same machine {0}!".format(config['srchv']['hostname']))

    if not offline and not (
                config['srchv']['hypervisor'] == 'kvm'
            and
                config['dsthv']['hypervisor'] == 'kvm'
        ):
        raise Exception('Online migration is only possible from KVM to KVM.')

    if not config['runpuppet'] and newip:
        raise Exception("Changing IP requires a Puppet run, don't pass --nopuppet.")

    downtime_network = None

    if not nolbdowntime and 'testtool_downtime' in config['vm']:
        if config['vm']['segment'] in ['af', 'aw', 'vn', 'none']:
            network_api = api.get('ip')
            for iprange in network_api.get_matching_ranges(config['vm']['intern_ip']):
                if iprange['belongs_to'] == None and iprange['type'] == 'private':
                    if downtime_network:
                        raise Exception('Unable to determine network for testtool downtime. Multiple networks found.')
                    downtime_network = iprange['range_id']
        else:
            downtime_network = config['vm']['segment']
        if not downtime_network:
            raise Exception('Unable to determine network for testtool downtime. No network found.')

    if newip:
        config['vm']['intern_ip'] = newip

    # Configure network
    config['vlan_tag'], offline_flag = get_vlan_info(
            config['vm'],
            config['srchv'],
            config['dsthv'],
            newip,
        )

    if not offline and offline_flag:
        raise Exception(
                'Online migration is not possible with the current network '
                'configuration.'
            )

    # First, get the VM information from the Serveradmin.  The next
    # step should validate that information.
    import_vm_config_from_admintool(config)

    # Import information about VM from source Hypervisor
    if config['srchv']['hypervisor'] == 'xen':
        execute(import_vm_config_from_xen, config, hosts=[config['srchv']['hostname']])
    elif config['srchv']['hypervisor'] == 'kvm':
        config['srchv_conn'] = get_virtconn(config['srchv']['hostname'], 'kvm')
        execute(import_vm_config_from_kvm, config, hosts=[config['srchv']['hostname']])
    else:
        raise Exception("Migration from Hypervisor type {0} is not supported".format(config['srchv']['hypervisor']))

    # Verify if config contains all the needed parameters
    check_vm_config(config)

    # Setup destination Hypervisor
    if config['dsthv']['hypervisor'] == 'xen':
        execute(setup_dsthv, config, offline, hosts=[config['dsthv']['hostname']])
    elif config['dsthv']['hypervisor'] == 'kvm':
        config['dsthv_conn'] = get_virtconn(config['dsthv']['hostname'], 'kvm')
        execute(setup_dsthv, config, offline, hosts=[config['dsthv']['hostname']])
    else:
        raise Exception("Migration to Hypervisor type {0} is not supported".format(config['dsthv']['hypervisor']))

    # Trigger pre-migration hooks
    send_signal('pre_migration', config, offline)

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

    # Trigger post-migration hooks
    send_signal('post_migration', config, offline)

    # Remove the existing VM
    source_vm.undefine()
    execute(
        remove_logical_volume,
        config['src_device'],
        hosts=[config['srchv']['hostname']],
    )

    close_virtconns()
    sleep(1) # For Paramiko's race condition.
