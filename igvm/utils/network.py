import logging
import urllib2

from fabric.api import abort

from adminapi import api

log = logging.getLogger(__name__)

class NetworkError(Exception):
    pass

def get_network_config(server):
    ip_api = api.get('ip')

    ip_info = {
        'address4': server['intern_ip'] if 'intern_ip' in server else None,
        'netmask4': None,
        'gateway4': None,
        'address6': server['primary_ip6'] if 'primary_ip6' in server else None,
        'netmask6': None,
        'gateway6': None,
        'vlan':     None,
        'segment':  None,
    }

    if ip_info['address4']:
        try:
            net4 = ip_api.get_network_settings(ip_info['address4'])
            ranges4 = ip_api.get_matching_ranges(ip_info['address4'])
            ip_info['segment']=ranges4[0]['segment']
        except urllib2.URLError:
            raise NetworkError('Admintool is down')
        except Exception as e:
            log.warn('Could not configure network automatically!')
            log.warn('Make sure that IP ranges are configured correctly in admintool.')
            abort('Error was: {0}'.format(e))
        else:
            # Copy settings from Admintool. Use only internal gateway, it should be enough for installation.
            ip_info['netmask4'] = net4['prefix_hi']
            ip_info['gateway4'] = net4['internal_gateway']

    if ip_info['address6']:
        try:
            net6 = ip_api.get_network_settings(ip_info['address6'])
        except urllib2.URLError:
            raise NetworkError('Admintool is down')
        except Exception as e:
            log.warn('Could not configure network automatically!')
            log.warn('Make sure that IP ranges are configured correctly in admintool.')
            abort('Error was: {0}'.format(e))
        else:
            # Copy settings from Admintool. Use only internal gateway, it should be enough for installation.
            ip_info['netmask6'] = net6['prefix_hi']
            ip_info['gateway6'] = net6['internal_gateway']

    ip_info['vlan'] = net4['vlan']

    return ip_info

def get_vlan_info(vm, srchv, dsthv, newip):
    # Prepare return value
    offline_flag = False

    # Handle changing of IP address
    if newip:
        offline_flag = True
        vm['intern_ip'] = newip

    # Get network configuration of all machines
    vm_net    = get_network_config(vm)
    dsthv_net = get_network_config(dsthv)

    if newip:
        vm['segment'] = vm_net['segment']

        log.info("Machine will be moved to new network, this enforces offline migration.")
        log.info("Segment: {0}, IP address: {1}, VLAN: {2}".format(vm['segment'], vm['intern_ip'], vm_net['vlan']))

    if 'network_vlans' in dsthv and dsthv['network_vlans']:
        if srchv and not srchv['network_vlans']:
            offline_flag = True
        if vm_net['vlan'] not in dsthv['network_vlans']:
            raise Exception('Destination Hypervisor does not support VLAN {0}.'.format(vm_net['vlan']))
    else:
        if srchv and srchv['network_vlans']:
            offline_flag = True
        if vm_net['vlan'] != dsthv_net['vlan']:
            raise Exception('Destination Hypervisor is not on same VLAN {0} as VM {1}.'.format(dsthv_net['vlan'], vm_net['vlan']))

        # Remove VLAN information, for untagged Hypervisors VM must be untagged too
        vm_net['vlan'] = None

    if offline_flag:
        log.info("VLAN configuration change enforces offline migration")

    return (vm_net['vlan'], offline_flag)
