from __future__ import division

import os
import sys
import re
from glob import glob
import math

from fabric.api import run
from igvm.utils.storage import get_logical_volumes
from igvm.utils.resources import get_cpuinfo

# get_server moved to host.py to avoid dependency cycles
# Many consumers still use get_server, so don't remove it!
import igvm.host
get_server = igvm.host.get_server


def init_vm_config(config):
    """ Put some hardcoded defaults into config dictionary.
        Those parameters are required only for new VM.

        Returns nothing, data is stored in 'config' dictionary."""

    config['swap_size'] = 1024
    config['mailname'] = config['vm_hostname'] + '.ig.local'
    config['dns_servers']=['10.0.0.102', '10.0.0.85', '10.0.0.83']


def get_vm_volume(vm):
    for lv in get_logical_volumes():
        if lv['name'] == vm['hostname']:
            if vm['disk_size_gib'] != int(math.ceil(lv['size_MiB'] / 1024)):
                raise Exception((
                    "Server disk_size_gib {0} on Serveradmin doesn't "
                    'match the volume size {1} MiB.'
                ).format(vm['disk_size_gib'], lv['size_MiB']))

            return lv['path']

    raise Exception('Unable to find source LV and determine its size.')


def import_vm_disk(config):
    lvs = get_logical_volumes()
    for lv in lvs:
        if lv['name'] == config['vm_hostname']:
            config['src_device'] = lv['path']
            config['disk_size_gib'] = int(math.ceil(lv['size_MiB'] / 1024))
            break
    else:
        raise Exception('Unable to find source LV and determine its size.')

def import_vm_config_from_admintool(config):
    """ Import configuration from Admintool.

        Returns nothing, data is stored in 'config' dictionary."""

    # TODO: Use those values directly instead of importing them
    config['mem'] = config['vm']['memory']
    config['num_cpu'] = config['vm']['num_cpu']
    config['os'] = config['vm']['os']
    config['disk_size_gib'] = config['vm']['disk_size_gib']

def import_vm_config_from_kvm(config):
    """ Import configuration from Hypervisor currently hosting the VM.

        Returns nothing, data is stored in 'config' dictionary."""

    # Some parameters must be retrieved from KVM.
    # Live migration will be performed, so they must be accurate.
    vm_obj = config['srchv_conn'].lookupByName(config['vm_hostname'])
    vm_info = vm_obj.info()

    config['max_mem'] = int(vm_info[1] / 1024)
    if config['max_mem'] == 0:
        config['max_mem'] = config['vm']['memory']

    config['mem']     = int(vm_info[2] / 1024)
    if config['mem'] == 0:
        config['mem'] = config['vm']['memory']

    config['num_cpu'] = vm_info[3]
    if config['num_cpu'] == 0:
        config['num_cpu'] = config['vm']['num_cpu']

    # Some we trust from Admintool
    config['os']      = config['srchv']['os']

    # The source device name must be retrieved from running source
    # hypervisor OS to be on the safer side.
    config['src_device'] = get_vm_volume(config['vm'])

def import_vm_config_from_xen(config):
    """ Import configuration from Hypervisor currently hosting the VM.

        Returns nothing, data is stored in 'config' dictionary."""

    # Some parameters must be retrieved from KVM.
    # Data in Admintool is currently known to be inaccurate
    config['num_cpu'] = int(run('xm list --long {0} | grep \'(online_vcpus \' | sed -E \'s/[ a-z\(_]+ ([0-9]+)\)/\\1/\''.format(config['vm_hostname'])))
    config['mem'] =     int(run('xm list --long {0} | grep \'(memory \' | sed -E \'s/[ a-z\(_]+ ([0-9]+)\)/\\1/\''.format(config['vm_hostname'])))
    config['max_mem'] = config['mem']

    # Some we trust from Admintool
    config['os']      = config['srchv']['os']

    # But not for disk size
    config['src_device'] = get_vm_volume(config['vm'])

def check_dsthv_vm(config):
    """ Check if VM is already defined on Destination Hypervisor.

        Returns nothing.
        Will raise an exception if VM already exists"""

    if config['dsthv']['hypervisor'] == 'kvm':
        for dom_id in config['dsthv_conn'].listDomainsID():
            dom = config['dsthv_conn'].lookupByID(dom_id)
            if dom.name() == config['vm_hostname']:
                raise Exception('VM {0} already defined on {1}'.format(config['vm_hostname'], config['dsthv_hostname']))

def check_dsthv_memory(config):
    """ Check various parameters of DstHV and VM memory.

        Returns nothing.
        Will raise an exception if there is not enough memory.
        Will modify config if all is fine. """
    if config['dsthv']['hypervisor'] == 'kvm':
        # Get amount of memory available to Hypervisor.
        # Start with what OS sees as total memory (not hardware installed memory)
        total_MiB = config['dsthv_conn'].getMemoryStats(-1)['total'] / 1024
        # Always keep extra 2GiB free for Hypervisor
        total_MiB -= 2*1024

        # Calculate memory used by other VMs.
        # We can not trust hv_conn.getFreeMemory(), sum up memory used by each VM instead
        used_KiB = 0
        for dom_id in config['dsthv_conn'].listDomainsID():
            dom = config['dsthv_conn'].lookupByID(dom_id)
            used_KiB += dom.info()[2]
        free_MiB = total_MiB - used_KiB/1024
        if config['mem'] > free_MiB:
            raise Exception('Not enough memory. Destination Hypervisor has {0}MiB but VM requires {1}MiB'.format(free_MiB, config['mem']))


def check_dsthv_cpu(config):
    cpuinfo = get_cpuinfo()
    num_cpus = len(cpuinfo)
    if config['num_cpu'] > num_cpus:
        raise Exception('Not enough CPUs. Destination Hypervisor has {0} but VM requires {1}.'.format(num_cpus, config['num_cpu']))

    assert num_cpus >= 4 and num_cpus <= 128, 'max_cpu is in sane range'
    config['max_cpu'] = min(24, num_cpus)

def check_vm_config(config):
    if 'mem' not in config:
        raise Exception('"mem" is not set.')

    if config['mem'] < 1:
        raise Exception('"mem" is not greater than 0.')

    if 'max_mem' not in config:
        if config['mem'] > 12288:
            config['max_mem'] = config['mem'] + 10240
        else:
            config['max_mem'] = 16384

    if config['max_mem'] < 1:
        raise Exception('"max_mem" is not greater than 0.')

    if config['max_mem'] <= config['mem']:
        # TODO: remove this dup of code and set it with libvirt api
        if config['mem'] > 12288:
            config['max_mem'] = config['mem'] + 10240
        else:
            config['max_mem'] = 16384

    if 'num_cpu' not in config:
        raise Exception('"num_cpu" is not set.')

    if config['num_cpu'] < 1:
        raise Exception('"num_cpu" is not greater than 0')

    if 'os' not in config:
        raise Exception('"os" is not set.')

    if 'disk_size_gib' not in config:
        raise Exception('"disk_size_gib" is not set.')

    if config['disk_size_gib'] < 1:
        raise Exception('"disk_size_gib" is not greater than 0')

    if 'image' not in config:
        config['image'] = config['os'] + '-base.tar.gz'
