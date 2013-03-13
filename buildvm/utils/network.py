import math
import urllib2
from itertools import chain

from fabric.contrib.console import confirm
from fabric.api import prompt, abort

from adminapi.utils import IP
from adminapi import api

class NetworkError(Exception):
    pass


_ip_regexp_base = r'(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)'
_ip_regexp = r'^{0}$'.format(_ip_regexp_base)
_ip_regexp_optional = r'^({0})?$'.format(_ip_regexp_base)

def _get_subnet(ip, ranges):
    try:
        return [r for r in ranges if r['belongs_to']][0]
    except IndexError:
        return False

def _get_uppernet(ip, ranges, segment=None):
    try:
        if segment:
            return [r for r in ranges if r['belongs_to'] is None and
                    r['segment'] == segment][0]
        else:
            return [r for r in ranges if r['belongs_to'] is None][0]
    except IndexError:
        return False

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

    if primary_ip.is_public():
        net = _get_uppernet(primary_ip, primary_ranges)
        if net:
            gateway_found = True
            ip_info[primary_ip]['gateway'] = IP(net['gateway'])
            ip_info[primary_ip]['netmask'] = _calc_netmask(net)
        else:
            raise NetworkError('No network found for IP {0}'.format(primary_ip))

    for ip in additional_ips:
        try:
            ranges = ip_api.get_matching_ranges(ip)
        except urllib2.URLError:
            raise NetworkError('Admintool is down')
        
        if ip.is_public():
            net = _get_uppernet(ip, ranges)
            if net:
                if not gateway_found:
                    gateway_found = True
                    ip_info[ip]['gateway'] = IP(net['gateway'])
                ip_info[ip]['netmask'] = _calc_netmask(net)
            else:
                raise NetworkError('No network found for IP {0}'.format(ip))
        else:
            pass

    routes = []
    if primary_ip.is_private():
        subnet = _get_subnet(primary_ip, primary_ranges)
        if not subnet:
            raise NetworkError('No network found for IP {0}'.format(primary_ip))

        uppernet = _get_uppernet(primary_ip, primary_ranges, subnet['segment'])
        if not uppernet:
            raise NetworkError('No upper network found for IP {0}'.format(
                    primary_ip))

        if not gateway_found:
            ip_info[primary_ip]['gateway'] = IP(subnet['gateway'])
        
        netmask = _calc_netmask(uppernet)
        ip_info[primary_ip]['netmask'] = netmask 

        # Route to other segments
        routes.append({
            'ip': '10.0.0.0',
            'netmask': '255.0.0.0',
            'gw': IP(uppernet['gateway'])
        })

    return {
        'ip_info': ip_info,
        'routes': routes
    }

def get_network_config(primary_ip, additional_ips):
    try:
        return _configure_ips(primary_ip, additional_ips)
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
            return {
                'ip_info': ip_info,
                'routes': routes
            }
        else:
            abort('Could not configure network')
