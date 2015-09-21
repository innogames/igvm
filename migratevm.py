#!/usr/bin/python
from __future__ import print_function

import argparse
import sys
import os

import adminapi
from adminapi.utils import IP
from adminapi.dataset import DatasetError, query

from managevm.migratevm import migratevm

parser = argparse.ArgumentParser(description='Migrate a virtual machine.')
parser.add_argument('guest',      metavar='hostname',   help='Hostname of the guest system')
parser.add_argument('--dsthv',    metavar='hostname',   help='Hostname of destination hypervisor')
parser.add_argument('--newip',    metavar='IP address', help='IP address to move VM to, in case you migrate between segments')
parser.add_argument('--nopuppet', action='store_true',  help='Skip running puppet in chroot before powering up')

args = parser.parse_args()

config = {}

adminapi.auth()

if args.guest:
    config['vm_hostname'] = args.guest

if args.dsthv:
    config['dsthv_hostname'] = args.dsthv

if args.newip:
    config['vm_new_ip'] = args.newip

if args.nopuppet:
    config['runpuppet'] = False
else:
    config['runpuppet'] = True

migratevm(config)

