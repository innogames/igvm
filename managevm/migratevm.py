import os, sys, re, time
from glob import glob

import libvirt
from time import strftime

from fabric.api import env, execute, run, settings
from fabric.context_managers import hide
from fabric.network import disconnect_all
from fabric.contrib.console import confirm

from adminapi.dataset import query, DatasetError
from adminapi import api

from managevm.signals import send_signal
from managevm.utils import raise_failure, fail_gracefully
from managevm.utils.config import *
from managevm.utils.hypervisor import *
from managevm.utils.network import get_vlan_info
from managevm.utils.portping import wait_until
from managevm.utils.preparevm import run_puppet
from managevm.utils.resources import get_ssh_keytypes
from managevm.utils.storage import *
from managevm.utils.units import convert_size
from managevm.utils.virtutils import close_virtconns

run = fail_gracefully(run)


def cleanup_srchv(config, offline):
    rename_old_vm(config['vm'], config['date'], offline, config['srchv']['hypervisor'])
    rename_logical_volume(config['src_device'], config['vm_hostname'], config['date'])

def setup_dsthv(config, offline):
    send_signal('setup_hardware', config)
    check_dsthv_cpu(config)
    check_dsthv_mem(config, config['dsthv']['hypervisor'])
    config['vm_block_dev'] = get_vm_block_dev(config['dsthv']['hypervisor'])
    config['dst_device'] = create_storage(config['vm_hostname'], config['disk_size_gib'])

    if offline:
        config['nc_port'] = netcat_to_device(config['dst_device'])

def add_dsthv_to_ssh(config):
    run('touch .ssh/known_hosts'.format(config['dsthv_hostname']))
    run('ssh-keygen -R {0}'.format(config['dsthv_hostname']))
    run('ssh-keyscan -t rsa {0} >> .ssh/known_hosts'.format(config['dsthv_hostname']))

def migrate_offline(config):
    add_dsthv_to_ssh(config)
    execute(shutdown_vm, config['vm'], config['srchv']['hypervisor'], hosts=config['srchv']['hostname'])
    execute(device_to_netcat, config['src_device'], config['disk_size_gib']*1024*1024*1024, config['dsthv_hostname'], config['nc_port'], hosts=config['srchv']['hostname'])

def start_offline_vm(config):

    if config['runpuppet']:
        vm_path = mount_temp(config['dst_device'], config['vm_hostname'])
        run_puppet( vm_path, config['vm_hostname'], False)
        umount_temp(vm_path)
        remove_temp(vm_path)

    # Signals are not used in hypervisor.py, so do not migrate this stuff there!
    hypervisor_extra = {}
    for extra in send_signal('hypervisor_extra', config, config['dsthv']['hypervisor']):
        hypervisor_extra.update(extra)

    create_definition(config['vm_hostname'], config['num_cpu'], config['mem'],
            config['max_mem'], config['vlan_tag'],
            config['dst_device'], config['dsthv']['hypervisor'], hypervisor_extra)

    send_signal('defined_vm', config, config['dsthv']['hypervisor'])

    start_machine(config['vm_hostname'], config['dsthv']['hypervisor'])

def migrate_virsh(config):

    # Unfortunately, virsh provides a global timeout, but what we need it to
    # timeout if it is catching up the dirtied memory.  To be in this stage,
    # it should have coped the initial disk and memory and changes on them.
    timeout = sum((
            # We assume the disk can be copied at 50 MBp/s;
            config['disk_size_gib'] * 1024 / 50,
            # the memroy at 100 MBp/s;
            config['mem'] / 100,
            # and 5 minutes more for other operations.
            5 * 60,
        ))

    migrate_cmd = ('virsh migrate'
            ' --live' # Do it live!
            ' --copy-storage-all'
            ' --persistent' # Define the VM on the new host
            ' --undefinesource' # Undefine the VM on the old host
            ' --change-protection' # Don't let the VM configuration to be changed
            ' --auto-converge' # Force convergence, otherwise migrations never end
            ' --domain {vm_hostname}'
            ' --abort-on-error' # Don't tolerate soft errors
            ' --desturi qemu+ssh://{dsthv_hostname}/system' # We need SSH agent forwarding
            ' --timeout {timeout}' # Force guest to suspend, if noting else helped
            ' --verbose'
        )

    add_dsthv_to_ssh(config)
    with settings(user='root', forward_agent=True):
        migrate_cmd = migrate_cmd.format(
                    vm_hostname    = config['vm_hostname'],
                    dsthv_hostname = config['dsthv_hostname'],
                    timeout        = timeout,
                )
        run(migrate_cmd)

def migratevm(vm_hostname, dsthv_hostname, newip=None, nopuppet=False, nolbdowntime=False, offline=False):
    config = {
        'vm_hostname': vm_hostname,
        # Character : is invalid for LV name, use - instead.
        'date': strftime("%Y-%m-%d_%H-%M-%S"),
        'dsthv_hostname': dsthv_hostname,
        'runpuppet': not nopuppet,
    }

    config['vm'] = get_vm(vm_hostname)
    config['srchv'] = get_srchv(config['vm']['xen_host'])
    config['dsthv'] = get_dsthv(dsthv_hostname)

    lb_api = api.get('lbadmin')

    if config['srchv']['hostname'] == config['dsthv']['hostname']:
        raise Exception("Source and destination Hypervisor is the same machine {0}!".format(config['srchv']['hostname']))

    # Configuration of Fabric:
    env.disable_known_hosts = True
    env.use_ssh_config = True
    env.always_use_pty = False
    env.forward_agent = True
    env.user = 'root'
    env.shell = '/bin/bash -c'

    if newip:
        config['vm']['intern_ip'] = newip

    # Configure network
    (config['vlan_tag'], offline_flag)= get_vlan_info(config['vm'], config['srchv'], config['dsthv'], newip)
    offline |= offline_flag

    # Enforce offline migration of one of Hypervisors is XEN
    if config['srchv']['hypervisor'] == "xen" or config['dsthv']['hypervisor'] == "xen":
        offline = True

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

    # Commit previously changed IP address and segment.
    if newip:
        config['vm'].commit()

    if not nolbdowntime:
        print "Downtiming testtool"
        config['vm']['testtool_downtime'] = True
        config['vm'].commit()
        lb_api.downtime_segment_push(config['vm']['segment'])

    # Finally migrate the VM
    if offline:
        execute(migrate_offline, config, hosts=[config['srchv']['hostname']])
        execute(start_offline_vm, config, hosts=[config['dsthv']['hostname']])
    else:
        execute(migrate_virsh, config, hosts=[config['srchv']['hostname']])

    if not nolbdowntime:
        print "Removing testtool downtime"
        config['vm']['testtool_downtime'] = False
        config['vm'].commit()
        lb_api.downtime_segment_push(config['vm']['segment'])

    # Rename resources on source hypervisor.
    execute(cleanup_srchv, config, offline, hosts=[config['srchv']['hostname']])

    # Update admintool information
    config['vm']['xen_host'] = config['dsthv']['hostname']
    config['vm']['num_cpu'] = config['num_cpu']
    config['vm']['memory'] = config['mem']
    config['vm'].commit()

    close_virtconns()
    disconnect_all()

