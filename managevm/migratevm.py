import os, sys, re, time
from glob import glob

import libvirt

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
from managevm.utils.portping import wait_until
from managevm.utils.preparevm import run_puppet
from managevm.utils.resources import get_meminfo, get_cpuinfo, get_ssh_keytypes
from managevm.utils.storage import *
from managevm.utils.units import convert_size
from managevm.utils.virtutils import close_virtconns

run = fail_gracefully(run)

def check_dsthv_mem(config):
    if config['dsthv']['hypervisor'] == 'kvm':
        conn = config['dsthv_conn']
        # Always keep extra 2GiB free
        free_MiB = (conn.getFreeMemory() / 1024 / 1024) - 2048
        if config['mem'] > (free_MiB):
            # Avoid ugly error messages
            close_virtconns()
            raise HypervisorError('Not enough memory. Destination Hypervisor has {0}MiB but VM requires {1}MiB'.format(free_MiB, config['mem']))
    # Add statements to check hypervisor different than kvm

def check_dsthv_cpu(config):
    cpuinfo = get_cpuinfo()
    num_cpus = len(cpuinfo)
    if config['num_cpu'] > num_cpus:
        raise Exception('Not enough CPUs. Destination Hypervisor has {0} but VM requires {1}.'.format(num_cpus, config['num_cpu']))

def setup_dsthv(config):
    send_signal('setup_hardware', config)
    check_dsthv_mem(config)
    check_dsthv_cpu(config)
    config['vm_block_dev'] = get_vm_block_dev(config['dsthv']['hypervisor'])
    config['dst_device'] = create_storage(config['vm_hostname'], config['disk_size_gib'])

    if config['migration_type'] == 'offline':
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
            config['max_mem'], config['network']['vlan'],
            config['dst_device'], config['dsthv']['hypervisor'], hypervisor_extra)

    send_signal('defined_vm', config, config['dsthv']['hypervisor'])

    start_machine(config['vm_hostname'], config['dsthv']['hypervisor'])

def migrate_virsh(config):
    migrate_cmd = ('virsh migrate'
            + ' --live' # Do it live!
            + ' --copy-storage-all'
            + ' --persistent' # Define the VM on the new host
            + ' --undefinesource' # Undefine the VM on the old host
            + ' --change-protection' # Don't let the VM configuration to be changed
            + ' --auto-converge' # Force convergence, otherwise migrations never end
            + ' --domain {vm_hostname}'
            + ' --abort-on-error' # Don't tolerate soft errors
            + ' --desturi qemu+ssh://{dsthv_hostname}/system' # We need SSH agent forwarding
            + ' --timeout ' + str(10 * 60) # Force guest to suspend after 10 minutes
            + ' --verbose'
            )

    add_dsthv_to_ssh(config)
    with settings(user='root', forward_agent=True):
        migrate_cmd = migrate_cmd.format(
                    vm_hostname    = config['vm_hostname'],
                    dsthv_hostname = config['dsthv_hostname'],
                )
        run(migrate_cmd)

def migratevm(config):
    if not set(['vm_hostname', 'dsthv_hostname', 'runpuppet']) <= set(config.keys()):
        raise Exception("vm_hostname, dsthv_hostname, runpuppet must be specified in config!")

    config['vm'] = get_vm(config['vm_hostname'])
    config['srchv'] = get_srchv(config['vm']['xen_host'])
    config['dsthv'] = get_dsthv(config['dsthv_hostname'])

    lb_api = api.get('lbadmin')

    if 'vm_new_ip' in config:
        config['vm']['intern_ip'] = config['vm_new_ip']
        # Verify if this IP can get its configuration.
        # VLAN will be used if any is found.
        config['network'] = get_network_config(config['vm'])
        # Update IP address in Admintool, if nothing else has failed.
        # Machine is reconfigured via Puppet and he reads the data from Admintool
        config['vm']['intern_ip'] = config['network']['address4']
        config['vm']['segment'] = config['network']['segment']
        config['vm'].commit()
        print("Machine will be moved to new network:")
        print("Segment: {0}, IP address: {1}, VLAN: {2}".format(config['network']['segment'], config['network']['address4'], config['network']['vlan']))

    if config['srchv']['hostname'] == config['dsthv']['hostname']:
        raise Exception("Source and destination Hypervisor is the same machine {0}!".format(config['srchv']['hostname']))
   
    # Configuration of Fabric:
    env.disable_known_hosts = True
    env.use_ssh_config = True
    env.always_use_pty = False
    env.forward_agent = True
    env.user = 'root'
    env.shell = '/bin/bash -c'

    # Determine method of migration:
    config['migration_type'] = 'online'
    if 'vm_new_ip' in config:
        config['migration_type'] = 'offline'
    if config['srchv']['hypervisor'] == "xen" or config['dsthv']['hypervisor'] == "xen":
        config['migration_type'] = 'offline'

    if config['srchv']['hypervisor'] == 'xen':
        execute(import_vm_config_from_xen, config, hosts=[config['srchv']['hostname']])
    elif config['srchv']['hypervisor'] == 'kvm':
        config['srchv_conn'] = get_virtconn(config['srchv']['hostname'], 'kvm')
        execute(import_vm_config_from_kvm, config, hosts=[config['srchv']['hostname']])
    else:
        raise Exception("Migration from Hypervisor type {0} is not supported".format(config['srchv']['hypervisor']))
    check_vm_config(config)

    if config['dsthv']['hypervisor'] == 'xen':
        execute(setup_dsthv, config, hosts=[config['dsthv']['hostname']])
    elif config['dsthv']['hypervisor'] == 'kvm':
        config['dsthv_conn'] = get_virtconn(config['dsthv']['hostname'], 'kvm')
        execute(setup_dsthv, config, hosts=[config['dsthv']['hostname']])
    else:
        raise Exception("Migration to Hypervisor type {0} is not supported".format(config['dsthv']['hypervisor']))

    if 'lbdowntime' in config:
        config['vm']['testtool_downtime'] = True
        config['vm'].commit()
        lbapi.downtime_segment_push(config['vm']['segment'])
        
    if config['migration_type'] == 'offline':
        execute(migrate_offline, config, hosts=[config['srchv']['hostname']])
        execute(start_offline_vm, config, hosts=[config['dsthv']['hostname']])
    elif config['migration_type'] == 'online':
        execute(migrate_virsh, config, hosts=[config['srchv']['hostname']])
    else:
        raise Exception("Migration type {0} is not supported".format(config['migration_type']))

    if 'lbdowntime' in config:
        config['vm']['testtool_downtime'] = False
        config['vm'].commit()
        lbapi.downtime_segment_push(config['vm']['segment'])

    # Update admintool information
    config['vm']['xen_host'] = config['dsthv']['hostname']
    config['vm'].commit()

    close_virtconns()
    disconnect_all()

