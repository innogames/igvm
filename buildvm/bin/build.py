#!/usr/bin/python2.6
from __future__ import print_function

import argparse
import sys
import os

#print('Disabled during the weekend')
#sys.exit(1)

import adminapi
from adminapi.utils import IP
from adminapi.dataset import DatasetError, query

from buildvm.main import setup

parser = argparse.ArgumentParser(description='Creates a new virtual machine.')
parser.add_argument('guest', metavar='guest', help='Hostname of the guest system')
parser.add_argument('--host', metavar='host', help='Hostname of the host system')
parser.add_argument('--image', metavar='image', help='Image file for the guest')
parser.add_argument('--ip', metavar='intern_ip', help='Internal IP of the guest')
parser.add_argument('--addip', metavar='additional_ip', action='append',
        help='Additional IPs of the guest. You can use this multiple times.')
parser.add_argument('--mem', metavar='memory', type=int,
        help='Memory of the guest in MiB')
parser.add_argument('--numcpu', metavar='numcpu', type=int,
        help='Number of CPUs for the guest')
parser.add_argument('--disksize', metavar='disksize', type=int,
        help='Disk size of the guest in MiB')
parser.add_argument('--boot', action='store_true', help='Boot after setup')
parser.add_argument('--postboot', metavar='postboot_script',
        help='Run postboot_script on the guest after first boot')
parser.add_argument('-o', metavar='key=value', nargs='+',
        type=lambda x: x.split('=', 1), help='Sets an option')

args = parser.parse_args()

config = {
    'hostname': args.guest,
    'swap_size': 1024,
    'mailname': args.guest + 'ig.local',
    'dns_servers': ['10.0.0.102', '10.0.0.85', '10.0.0.83']
}

if args.o:
    for key, value in args.o:
        config[key] = value

adminapi.auth()

try:
    server = query(hostname=args.guest).get()
except DatasetError:
    print("Server '{0}' not found".format(args.guest), file=sys.stderr)
    server = {
        'intern_ip': IP(args.ip),
        'additional_ips': set(map(IP, args.addip))
    }

config['server'] = server

if args.host:
    config['host'] = args.host
else:
    xen_host = server.get('xen_host')
    if xen_host:
        config['host'] = xen_host

if args.mem:
    config['mem'] = args.mem
else:
    mem = server.get('memory')
    if mem:
        config['mem'] = mem

if args.numcpu:
    config['num_cpu'] = args.numcpu
else:
    num_cpu = server.get('num_cpu')
    if num_cpu:
        config['num_cpu'] = num_cpu

if args.disksize:
    config['disk_size'] = args.disksize
else:
    disk_size = server.get('disk_size')
    if disk_size:
        config['disk_size'] = disk_size

if args.image:
    config['image'] = args.image

if args.boot:
    config['boot'] = True
else:
    config['boot'] = False

if args.postboot:
    config['postboot_script'] = args.postboot
    config['boot'] = True

setup(config)
