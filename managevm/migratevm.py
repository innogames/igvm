import os, sys, re
from glob import glob

import libvirt

from fabric.api import env, execute, run, settings
from fabric.network import disconnect_all
from fabric.contrib.console import confirm
#
from adminapi.dataset import query, DatasetError
#
from managevm.utils import raise_failure, fail_gracefully
from managevm.utils.config import *
from managevm.utils.units import convert_size
from managevm.utils.resources import get_meminfo, get_cpuinfo, get_ssh_keytypes
from managevm.utils.storage import get_vm_block_dev, create_storage
#from managevm.utils.image import download_image, extract_image, get_images
#from managevm.utils.network import get_network_config
#from managevm.utils.preparevm import prepare_vm, copy_postboot_script, run_puppet, block_autostart, unblock_autostart
from managevm.utils.hypervisor import *
#from managevm.utils.portping import wait_until
from managevm.utils.virtutils import close_virtconns
from managevm.signals import send_signal

run = fail_gracefully(run)

def setup_dsthv(config):
    send_signal('setup_hardware', config)
    check_dsthv_mem(config)
    check_dsthv_cpu(config)

    config['vm_block_dev'] = get_vm_block_dev(config['dsthv']['hypervisor'])

    device = create_storage(config['vm_hostname'], config['disk_size_gib'])

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

    with settings(user='root', forward_agent=True):
        migrate_cmd = migrate_cmd.format(
                    vm_hostname    = config['vm_hostname'],
                    dsthv_hostname = config['dsthv_hostname'],
                )
        # Ensure that virsh does not complain
        run('ssh-keyscan -t rsa {0} >> .ssh/known_hosts'.format(config['dsthv_hostname']))
        run(migrate_cmd)

def migratevm(config):
    if not set(['vm_hostname', 'dsthv_hostname', 'runpuppet']) == set(config.keys()):
        raise Exception("vm_hostname, dsthv_hostname, runpuppet must be specified in config!")

    config['vm'] = get_vm(config['vm_hostname'])
    config['srchv'] = get_srchv(config['vm']['xen_host'])
    config['dsthv'] = get_dsthv(config['dsthv_hostname'])

    if config['srchv']['hostname'] == config['dsthv']['hostname']:
        raise Exception("Source and destination Hypervisor is the same machine!")
   
    # Configuration of Fabric:
    env.disable_known_hosts = True
    env.use_ssh_config = True
    env.always_use_pty = False
    env.forward_agent = True
    env.user = 'root'
    env.shell = '/bin/bash -c'

    if config['srchv']['hypervisor'] == 'kvm':
        if config['dsthv']['hypervisor'] == 'kvm':
            # Connect to both hosts via libvirt
            config['srchv_conn'] = get_virtconn(config['srchv']['hostname'], 'kvm')
            config['dsthv_conn'] = get_virtconn(config['dsthv']['hostname'], 'kvm')

            # Import configuration from source Hypervisor
            execute(import_vm_config_from_kvm, config, hosts=[config['srchv']['hostname']])
            check_vm_config(config) 

            # Create all things necessary on destination Hypervisor
            execute(setup_dsthv, config, hosts=[config['dsthv']['hostname']])

            # Connect to source Hypervisor again to perform migration
            execute(migrate_virsh, config, hosts=[config['srchv']['hostname']])

            # Update admintool information
            config['vm']['xen_host'] = config['dsthv']['hostname']
            config['vm'].commit()
        else:
            raise Exception("Migration from {0} to {1} is not supported".format(config['srchv']['hypervisor'], config['dsthv']['hypervisor']))
    else:
        raise Exception("Migration from {0} to {1} is not supported".format(config['srchv']['hypervisor'], config['dsthv']['hypervisor']))

    close_virtconns()
    disconnect_all()

