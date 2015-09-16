import math
import urllib2
from itertools import chain
from socket import inet_ntoa
from struct import pack

from fabric.contrib.console import confirm
from fabric.api import prompt, abort

from adminapi.dataset import query
from adminapi.dataset.exceptions import DatasetError
from adminapi.utils import IP
from adminapi import api

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
    }

    if ip_info['address4']:
        try:
            net4 = ip_api.get_network_settings(ip_info['address4'])
        except urllib2.URLError:
            raise NetworkError('Admintool is down')
        except Exception as e:
            print('Could not configure network automatically!')
            print('Make sure that IP ranges are configured correctly in admintool.')
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
            print('Could not configure network automatically!')
            print('Make sure that IP ranges are configured correctly in admintool.')
            abort('Error was: {0}'.format(e))
        else:
            # Copy settings from Admintool. Use only internal gateway, it should be enough for installation.
            ip_info['netmask6'] = net6['prefix_hi']
            ip_info['gateway6'] = net6['internal_gateway']

    ip_info['vlan'] = net4['vlan']

    return ip_info

