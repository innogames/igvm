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


_ip_regexp_base = r'(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)'
_ip_regexp = r'^{0}$'.format(_ip_regexp_base)
_ip_regexp_optional = r'^({0})?$'.format(_ip_regexp_base)

def _get_subnet(ip, ranges):
    if not ranges:
        return False
    
    try:
        return [r for r in ranges if r['belongs_to']][0]
    except IndexError:
        return min(ranges, key=lambda x: x['max'] - x['min'])

def _calc_netmask(iprange):
    host_bits = int(math.ceil(math.log(iprange['max'] - iprange['min'], 2)))
    return IP(-1 << host_bits)

def _configure_ips(primary_ip, additional_ips):
    ip_api = api.get('ip')
    
    ip_info = {}
    ip_info[primary_ip] = {
        'gateway': None,
        'ip': primary_ip
    }
    for ip in additional_ips:
        ip_info[ip] = {
            'gateway': None,
            'ip': ip
        }
    
    gateway_found = False
    try:
        primary_ranges = ip_api.get_matching_ranges(primary_ip)
    except urllib2.URLError:
        raise NetworkError('Admintool is down')

    routes = []
    net = ip_api.get_network_settings(primary_ip)
    ip_info[primary_ip]['gateway'] = net['default_gateway']
    ip_info[primary_ip]['netmask'] = net['netmask']

    # Route to other segments
    if net['default_gateway']:
        routes.append({
            'ip': '10.0.0.0',
            'netmask': '255.0.0.0',
            'gw': net['default_gateway'],
        })

    return {
        'ip_info': ip_info,
        'routes': routes
    }

def get_network_config(server):
    primary_ip = server['intern_ip']
    additional_ips = server['additional_ips']
    network_config = {}
    
    network_config['loadbalancer'] = []
    loadbalancer_successful = True
    for lb_host in server.get('loadbalancer', set()):
        try:
            loadbalancer = query(hostname=lb_host).restrict('intern_ip').get()
        except DatasetError:
            print('Could not configure loadbalancer: {0}'.format(lb_host))
            loadbalancer_successful = False
        else:
            network_config['loadbalancer'].append(loadbalancer['intern_ip'])

    if not loadbalancer_successful:
        if not confirm('Could not configure loadbalancer. Continue?'):
            abort('Aborting on request')

    try:
        network_config.update(_configure_ips(primary_ip, additional_ips))
        return network_config
    except NetworkError as e:
        print('Could not configure network automatically!')
        print('Make sure that IP ranges are configured correctly in admintool.')
        print('You should have a *.scope-internal ip range in your segment.')
        print('Error was: {0}'.format(e))
        
        if confirm('Configure network manually?'):
            ip_info = {}
            for ip in chain([primary_ip], additional_ips):
                netmask = prompt('Netmask for {0}:'.format(ip),
                        validate=_ip_regexp)
                gateway = prompt('Gateway for {0}:'.format(ip),
                        validate=_ip_regexp_optional)
                ip_info[ip] = {
                    'ip': ip,
                    'netmask': IP(netmask)
                }
                if gateway:
                    ip_info[ip]['gateway'] = IP(gateway)
                else:
                    ip_info[ip]['gateway'] = None
            print('Thanks for IP configuration!')
            print('Now you can add some routes. Just leave IP empty to quit')
            routes = []
            while True:
                ip = prompt('IP:', validate=_ip_regexp_optional)
                if not ip:
                    break
                netmask = prompt('Netmask:', validate=_ip_regexp)
                gateway = prompt('Gateway:', validate=_ip_regexp)
                routes.append({
                    'ip': ip,
                    'netmask': netmask,
                    'gateway': gateway
                })
            network_config['ip_info'] = ip_info
            network_config['routes'] = routes
            return network_config
        else:
            abort('Could not configure network')
